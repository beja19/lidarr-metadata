# coding=utf-8
"""
Local search and the background queue (enrichment.py), with stand-ins for the caches, the
database and the outside providers: nothing here reaches the network or Postgres.
"""
import asyncio
import time
from datetime import timedelta

import pytest

from lidarrmetadata import enrichment
from lidarrmetadata import provider
from lidarrmetadata import util

FUTURE = timedelta(days=20)


def now():
    return provider.utcnow()


class FakeCache:
    def __init__(self, rows=None):
        self.rows = dict(rows or {})
        self.reads = []

    async def get(self, key):
        self.reads.append(key)
        if key in self.rows:
            return self.rows[key]
        return None, now()

    async def set(self, key, value, ttl=None):
        self.rows[key] = (value, now() + timedelta(seconds=ttl))


class FakeDb:
    def __init__(self, artists=(), albums=()):
        self.artists = {a['id']: a for a in artists}
        self.albums = {a['id']: a for a in albums}
        self.artist_calls = []

    async def get_artists_by_id(self, ids):
        self.artist_calls.append(list(ids))
        # The database answers in its own order
        return [dict(self.artists[i]) for i in reversed(ids) if i in self.artists] or None

    async def get_release_groups_by_id(self, ids):
        return [dict(self.albums[i]) for i in ids if i in self.albums] or None

    async def redirect_old_artist_id(self, artist_id):
        return None

    async def redirect_old_release_group_id(self, rg_id):
        return None


class NoCalls:
    """A provider that fails the test if anything outside is asked."""

    def __init__(self, enabled=True):
        self.enabled = enabled
        self._paused_until = 0.0

    def is_enabled(self):
        return self.enabled

    def _paused(self):
        return time.monotonic() < self._paused_until

    async def get_artist_images(self, mbid):
        raise AssertionError('outside call during a search')

    async def get_artist_overview(self, *args, **kwargs):
        raise AssertionError('outside call during a search')


def wiki(url):
    return [{'type': 'wikidata', 'target': url}]


@pytest.fixture
def setup(monkeypatch):
    def install(db, wiki_rows=None, tadb_rows=None, wikipedia=None, tadb=None):
        wiki_cache = FakeCache(wiki_rows)
        tadb_cache = FakeCache(tadb_rows)
        monkeypatch.setattr(util, 'WIKI_CACHE', wiki_cache)
        monkeypatch.setattr(util, 'TADB_CACHE', tadb_cache)
        monkeypatch.setattr(enrichment, '_wikipedia', lambda: wikipedia or NoCalls())
        monkeypatch.setattr(enrichment, '_tadb', lambda: tadb or NoCalls())
        monkeypatch.setattr(provider, 'get_providers_implementing', lambda cls: [db])
        queue = enrichment.Enricher(limit=100, tadb_gap=0, retry_seconds=0.05)
        monkeypatch.setattr(enrichment, 'ENRICHER', queue)
        return wiki_cache, tadb_cache, queue
    return install


TADB_RECORD = {'strArtistThumb': 'https://r2.theaudiodb.com/images/media/artist/thumb/a.jpg',
               'strBiographyEN': 'From TheAudioDB'}


@pytest.mark.asyncio
async def test_search_artists_reads_caches_only_and_queues_the_rest(setup):
    db = FakeDb(artists=[
        {'id': 'a1', 'artistname': 'Cached', 'links': wiki('https://www.wikidata.org/wiki/Q1')},
        {'id': 'a2', 'artistname': 'New', 'links': wiki('https://www.wikidata.org/wiki/Q2')},
    ])
    _, _, queue = setup(db,
                        wiki_rows={'https://www.wikidata.org/wiki/Q1': ('From Wikipedia', now() + FUTURE)},
                        tadb_rows={'a1': (TADB_RECORD, now() + FUTURE)})

    artists, scores, _ = await enrichment.search_artists(
        [{'Id': 'a2', 'Score': 90}, {'Id': 'a1', 'Score': 80}, {'Id': 'gone', 'Score': 70}])

    assert [a['id'] for a in artists] == ['a2', 'a1']  # Solr's order, missing IDs dropped
    assert scores == [90, 80]
    assert artists[1]['overview'] == 'From Wikipedia'
    assert artists[1]['images'][0]['CoverType'] == 'Poster'
    assert artists[0]['overview'] == '' and artists[0]['images'] == []
    assert queue.status()['queue'] == 1 and (enrichment.ARTIST, 'a2') in queue._keys


@pytest.mark.asyncio
async def test_expired_rows_are_served_and_queued(setup):
    db = FakeDb(artists=[{'id': 'a1', 'artistname': 'Old', 'links': wiki('https://www.wikidata.org/wiki/Q1')}])
    _, _, queue = setup(db,
                        wiki_rows={'https://www.wikidata.org/wiki/Q1': ('Old text', now() - FUTURE)},
                        tadb_rows={'a1': (TADB_RECORD, now() - FUTURE)})

    artists, _, _ = await enrichment.search_artists([{'Id': 'a1', 'Score': 1}])

    assert artists[0]['overview'] == 'Old text'
    assert artists[0]['images']
    assert queue.status()['queue'] == 1


@pytest.mark.asyncio
async def test_stored_empty_summary_counts_as_known(setup):
    db = FakeDb(artists=[{'id': 'a1', 'artistname': 'Quiet', 'links': wiki('https://www.wikidata.org/wiki/Q1')}])
    _, _, queue = setup(db,
                        wiki_rows={'https://www.wikidata.org/wiki/Q1': ('', now() + FUTURE)},
                        tadb_rows={'a1': (None, now() + FUTURE)})

    artists, _, _ = await enrichment.search_artists([{'Id': 'a1', 'Score': 1}])

    assert artists[0]['overview'] == ''
    assert queue.status()['queue'] == 0


@pytest.mark.asyncio
async def test_search_albums_builds_artists_locally(setup):
    db = FakeDb(
        artists=[{'id': 'a1', 'artistname': 'Artist', 'links': []}],
        albums=[{'id': 'r1', 'title': 'Album', 'artistids': ['a1'], 'links': wiki('https://www.wikidata.org/wiki/Q9'),
                 'images': [{'CoverType': 'Cover', 'Url': 'https://coverartarchive.org/release/x/1-1200.jpg'}]},
                {'id': 'r2', 'title': 'Same artist', 'artistids': ['a1'], 'links': []}])
    _, _, queue = setup(db, tadb_rows={'a1': (None, now() + FUTURE)})

    albums, scores, _ = await enrichment.search_albums([{'Id': 'r1', 'Score': 5}, {'Id': 'r2', 'Score': 4}])

    assert [a['id'] for a in albums] == ['r1', 'r2']
    assert all('artistids' not in a for a in albums)
    assert albums[0]['artists'][0]['artistname'] == 'Artist'
    assert albums[0]['images'][0]['CoverType'] == 'Cover'
    assert db.artist_calls == [['a1']]  # one database call for every album's artists
    assert queue._keys == {(enrichment.ALBUM, 'r1')}  # r1's Wikipedia text is missing; r2 has no link


def test_response_url_without_cache_host(monkeypatch):
    link = 'https://coverartarchive.org/release/x/1-1200.jpg'
    monkeypatch.setattr(provider.CONFIG, 'IMAGE_CACHE_HOST', '')
    assert provider.response_url(link) == link
    monkeypatch.setattr(provider.CONFIG, 'IMAGE_CACHE_HOST', 'images.example')
    assert provider.response_url(link) == f'http://images.example/cache/{link}'


@pytest.mark.asyncio
async def test_wikipedia_stored_empty_summary_is_not_fetched_again(monkeypatch):
    monkeypatch.setattr(util, 'WIKI_CACHE', FakeCache({'https://www.wikidata.org/wiki/Q1': ('', now() + FUTURE)}))
    wikipedia = provider.WikipediaProvider()

    async def fetch(url):
        raise AssertionError('fetched again')

    monkeypatch.setattr(wikipedia, 'wikidata_get_summary_from_url', fetch)
    summary, _ = await wikipedia.get_artist_overview('https://www.wikidata.org/wiki/Q1')
    assert summary == ''


class QueueHarness:
    def __init__(self, monkeypatch, outcomes):
        self.queue = enrichment.Enricher(limit=3, tadb_gap=0, retry_seconds=0.05)
        self.worked = []
        self.outcomes = outcomes

        async def work(stop, kind, mbid):
            self.worked.append(mbid)
            outcome = self.outcomes.get(mbid, True)
            if isinstance(outcome, list):
                outcome = outcome.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        monkeypatch.setattr(self.queue, '_work', work)

    async def run_until(self, done, timeout=3):
        stop = asyncio.Event()
        task = asyncio.create_task(self.queue.run(stop))
        deadline = time.monotonic() + timeout
        while not done() and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, 3)


@pytest.mark.asyncio
async def test_queue_dedups_bounds_retries_and_contains_errors(monkeypatch):
    harness = QueueHarness(monkeypatch, {'bad': [False, False], 'boom': RuntimeError('broken page')})
    queue = harness.queue
    queue.add(enrichment.ARTIST, 'ok')
    queue.add(enrichment.ARTIST, 'ok')  # a repeat
    queue.add(enrichment.ARTIST, 'bad')
    queue.add(enrichment.ALBUM, 'boom')
    queue.add(enrichment.ALBUM, 'over')  # past the limit of 3

    counts = queue.counts
    await harness.run_until(lambda: counts['done'] + counts['failed'] >= 3)

    assert counts['queued'] == 3 and counts['dropped'] == 1
    assert harness.worked.count('ok') == 1
    assert harness.worked.count('bad') == 2  # retried once, then dropped
    assert counts['failed'] == 2 and counts['errors'] == 2  # 'boom' raised on both tries
    assert queue.status()['queue'] == 0 and queue._keys == set()
    assert not queue.running


@pytest.mark.asyncio
async def test_queue_waits_out_a_wikimedia_pause(monkeypatch):
    wikipedia = NoCalls()
    wikipedia._paused_until = time.monotonic() + 0.3
    monkeypatch.setattr(enrichment, '_wikipedia', lambda: wikipedia)
    queue = enrichment.Enricher(limit=10, tadb_gap=0, retry_seconds=1)
    stop = asyncio.Event()

    started = time.monotonic()
    assert await queue._wait_for_wikimedia(stop)
    assert time.monotonic() - started >= 0.25
    assert queue.waiting_for is None


@pytest.mark.asyncio
async def test_theaudiodb_refusal_backs_off(monkeypatch):
    tadb_cache = FakeCache()

    class RefusingTadb(NoCalls):
        calls = 0

        async def get_artist_images(self, mbid):
            RefusingTadb.calls += 1
            # get_data stores a refusal for the providers' error time
            await tadb_cache.set(mbid, None, ttl=1800)
            return []

    monkeypatch.setattr(util, 'TADB_CACHE', tadb_cache)
    monkeypatch.setattr(enrichment, '_tadb', lambda: RefusingTadb())
    queue = enrichment.Enricher(limit=10, tadb_gap=0, retry_seconds=1)
    stop = asyncio.Event()

    assert await queue._tadb_images(stop, 'a1') is False
    assert queue._tadb_ready - time.monotonic() > 55  # a minute before the next call
    assert await queue._tadb_images(stop, 'a1') is True  # cached now: no call, no wait
    assert RefusingTadb.calls == 1
