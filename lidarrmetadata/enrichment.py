"""
Search answers from the MusicBrainz mirror alone, and a background queue for biographies
and photos (estate wiki, planning/metadata-search-local.md).

Searches build artists and albums with the database queries the lookups use, and read
biographies and photos only from the Wikipedia and TheAudioDB caches. Anything missing or
expired is queued, and one background task fetches it through the providers' own methods,
so the Wikimedia cap and pause apply and the answers land in the same caches. The task works
one item at a time, so it holds at most one of Wikimedia's slots and page lookups keep the
rest.
"""

import asyncio
import collections
import heapq
import logging
import time
from datetime import timedelta

from lidarrmetadata import config
from lidarrmetadata import provider
from lidarrmetadata import util

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CONFIG = config.get_config()

ARTIST = 'artist'
ALBUM = 'album'


def _now():
    return provider.utcnow()


def _enabled(cls):
    return next((p for p in provider.get_providers_implementing(cls) if p.is_enabled()), None)


def _wikipedia():
    return _enabled(provider.WikipediaProvider)


def _tadb():
    return _enabled(provider.TheAudioDbProvider)


def wiki_link(links):
    """The link api.get_overview uses: Wikidata first, then Wikipedia."""
    links = links or []
    wikidata = next((link for link in links if 'wikidata' in link.get('type', '')), None)
    wikipedia = next((link for link in links if 'wikipedia' in link.get('type', '')), None)
    chosen = wikidata or wikipedia
    return chosen['target'] if chosen else None


async def _read(cache, key):
    """(value, expires) from an expiry cache; a missing key reads as (None, now)."""
    got = await cache.get(key)
    if not got:
        return None, _now()
    return got


async def cached_overview(links, mbid=None):
    """
    The biography from the caches only, as api.get_overview would build it, and whether every
    part read was present and unexpired. A stored empty summary counts as present.
    """
    now = _now()
    fresh = True
    text = ''

    url = wiki_link(links)
    if url and _wikipedia():
        value, expires = await _read(util.WIKI_CACHE, url)
        if value is None or expires <= now:
            fresh = False
        text = value or ''

    tadb = _tadb()
    if not text and mbid and tadb:
        record, expires = await _read(util.TADB_CACHE, mbid)
        if expires <= now:
            fresh = False
        text = provider.TheAudioDbProvider.parse_artist_overview(record)

    return text, fresh


async def cached_artist_images(mbid):
    """TheAudioDB's photos from its cache only, and whether that entry is present and unexpired."""
    if not _tadb():
        return [], True
    record, expires = await _read(util.TADB_CACHE, mbid)
    return provider.TheAudioDbProvider.parse_artist_images(record), expires > _now()


async def _complete_artist(artist):
    (images, images_fresh), (overview, overview_fresh) = await asyncio.gather(
        cached_artist_images(artist['id']),
        cached_overview(artist.get('links'), artist['id']))
    artist['images'] = images
    artist['overview'] = overview
    if not (images_fresh and overview_fresh):
        ENRICHER.add(ARTIST, artist['id'])
    return images_fresh and overview_fresh


def _validity(all_fresh):
    seconds = CONFIG.CACHE_TTL['cloudflare'] if all_fresh else CONFIG.CACHE_TTL['provider_error']
    return _now() + timedelta(seconds=seconds)


async def search_artists(hits):
    """Artists for Solr hits ({'Id', 'Score'}), in hit order: (artists, scores, validity)."""
    if not hits:
        return [], [], _validity(True)
    db = provider.get_providers_implementing(provider.ArtistByIdMixin)[0]
    found = await db.get_artists_by_id([hit['Id'] for hit in hits]) or []
    by_id = {artist['id']: artist for artist in found}

    ordered = [(by_id[hit['Id']], hit['Score']) for hit in hits if hit['Id'] in by_id]
    fresh = await asyncio.gather(*[_complete_artist(artist) for artist, _ in ordered])
    return [artist for artist, _ in ordered], [score for _, score in ordered], _validity(all(fresh))


async def search_albums(hits):
    """Release groups for Solr hits ({'Id', 'Score'}), with their artists: (albums, scores, validity)."""
    if not hits:
        return [], [], _validity(True)
    db = provider.get_providers_implementing(provider.ReleaseGroupByIdMixin)[0]
    found = await db.get_release_groups_by_id([hit['Id'] for hit in hits]) or []
    by_id = {album['id']: album for album in found}
    ordered = [(by_id[hit['Id']], hit['Score']) for hit in hits if hit['Id'] in by_id]

    artist_ids = list(dict.fromkeys(aid for album, _ in ordered for aid in album.get('artistids', [])))
    artist_db = provider.get_providers_implementing(provider.ArtistByIdMixin)[0]
    artists = await artist_db.get_artists_by_id(artist_ids) or [] if artist_ids else []
    artists_by_id = {artist['id']: artist for artist in artists}

    async def complete_album(album):
        overview, fresh = await cached_overview(album.get('links'))
        album['overview'] = overview
        if not fresh:
            ENRICHER.add(ALBUM, album['id'])
        album['artists'] = [artists_by_id[aid] for aid in album.pop('artistids', []) if aid in artists_by_id]
        return fresh

    results = await asyncio.gather(
        *[complete_album(album) for album, _ in ordered],
        *[_complete_artist(artist) for artist in artists])
    return [album for album, _ in ordered], [score for _, score in ordered], _validity(all(results))


class Enricher:
    """
    The background queue. Items are (kind, mbid); a repeat while one is queued, running or
    waiting to retry is dropped, and the queue is bounded. A failed item is retried once after
    the providers' error time; one that fails again is dropped and counted.
    """

    def __init__(self, limit, tadb_gap, retry_seconds):
        self._limit = limit
        self._tadb_gap = tadb_gap
        self._retry_seconds = retry_seconds
        self._queue = collections.deque()
        self._keys = set()
        self._retries = []  # heap of (ready monotonic time, kind, mbid)
        self._retried = set()
        self._wake = None
        self._tadb_ready = 0.0
        self._tadb_refused = False
        self.running = False
        self.waiting_for = None  # 'wikimedia' or 'theaudiodb' while waiting out a refusal
        self.counts = collections.Counter()

    def add(self, kind, mbid):
        key = (kind, mbid)
        if key in self._keys:
            return
        if len(self._queue) + len(self._retries) >= self._limit:
            self.counts['dropped'] += 1
            return
        self._keys.add(key)
        self._queue.append(key)
        self.counts['queued'] += 1
        if self._wake is not None:
            self._wake.set()

    def status(self):
        return {
            'running': self.running,
            'queue': len(self._queue),
            'retrying': len(self._retries),
            'waitingFor': self.waiting_for,
            **{name: self.counts[name] for name in ('queued', 'done', 'retried', 'failed', 'dropped', 'errors')},
        }

    def _next(self):
        if self._retries and self._retries[0][0] <= time.monotonic():
            _, kind, mbid = heapq.heappop(self._retries)
            return kind, mbid
        if self._queue:
            return self._queue.popleft()
        return None

    async def _sleep(self, stop, seconds):
        """Waits up to seconds, ending early on stop; returns False once stopping."""
        if seconds > 0:
            try:
                await asyncio.wait_for(stop.wait(), timeout=seconds)
            except asyncio.TimeoutError:
                pass
        return not stop.is_set()

    async def _idle(self, stop):
        timeout = 1.0
        if self._retries:
            timeout = max(0.0, min(timeout, self._retries[0][0] - time.monotonic()))
        self._wake.clear()
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    async def _wait_for_wikimedia(self, stop):
        wikipedia = _wikipedia()
        while wikipedia and wikipedia._paused() and not stop.is_set():
            self.waiting_for = 'wikimedia'
            await self._sleep(stop, min(1.0, wikipedia._paused_until - time.monotonic()))
        self.waiting_for = None
        return not stop.is_set()

    async def _tadb_images(self, stop, mbid):
        """Fetches TheAudioDB's entry unless cached and unexpired; False if it was refused."""
        tadb = _tadb()
        if not tadb:
            return True
        _, expires = await _read(util.TADB_CACHE, mbid)
        if expires > _now():
            return True
        if self._tadb_refused:
            self.waiting_for = 'theaudiodb'
        carry_on = await self._sleep(stop, self._tadb_ready - time.monotonic())
        self.waiting_for = None
        self._tadb_refused = False
        if not carry_on:
            return True
        self._tadb_ready = time.monotonic() + self._tadb_gap
        await tadb.get_artist_images(mbid)
        _, expires = await _read(util.TADB_CACHE, mbid)
        # A refusal is cached for the providers' error time, an answer for 30 days. After a
        # refusal TheAudioDB asks for a minute's wait.
        if expires - _now() < timedelta(days=1):
            self._tadb_refused = True
            self._tadb_ready = time.monotonic() + 60
            return False
        return True

    async def _work(self, stop, kind, mbid):
        """Fetches one item; True when everything it needed is now cached."""
        if not await self._wait_for_wikimedia(stop):
            return True
        # Imported here: api sets up the providers at import, and app imports both
        from lidarrmetadata import api

        if kind == ARTIST:
            db = provider.get_providers_implementing(provider.ArtistByIdMixin)[0]
            found = await db.get_artists_by_id([mbid])
            if not found:
                return True
            links = found[0].get('links')
            images_ok = await self._tadb_images(stop, mbid)
            await api.get_overview(links, mbid)
            _, fresh = await cached_overview(links, mbid)
            return images_ok and fresh

        db = provider.get_providers_implementing(provider.ReleaseGroupByIdMixin)[0]
        found = await db.get_release_groups_by_id([mbid])
        if not found:
            return True
        links = found[0].get('links')
        await api.get_overview(links)
        _, fresh = await cached_overview(links)
        return fresh

    def _finish(self, key, ok):
        if ok:
            self.counts['done'] += 1
        elif key not in self._retried:
            self._retried.add(key)
            self.counts['retried'] += 1
            heapq.heappush(self._retries, (time.monotonic() + self._retry_seconds, *key))
            return
        else:
            self.counts['failed'] += 1
        self._retried.discard(key)
        self._keys.discard(key)

    async def run(self, stop):
        """The background task: works the queue until stop is set."""
        self._wake = asyncio.Event()
        self.running = True
        logger.info('Enrichment queue started')
        try:
            while not stop.is_set():
                key = self._next()
                if key is None:
                    await self._idle(stop)
                    continue
                try:
                    ok = await self._work(stop, *key)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.counts['errors'] += 1
                    logger.exception(f'Enrichment of {key[0]} {key[1]} failed')
                    ok = False
                if stop.is_set():
                    break
                self._finish(key, ok)
        finally:
            self.running = False
            logger.info('Enrichment queue stopped')


ENRICHER = Enricher(limit=CONFIG.ENRICHMENT_QUEUE_LIMIT,
                    tadb_gap=CONFIG.ENRICHMENT_TADB_GAP,
                    retry_seconds=CONFIG.CACHE_TTL['provider_error'])
