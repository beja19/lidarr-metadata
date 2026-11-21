"""
Utility functionality that isn't specific to a given module
"""

import abc
import copy
import time
import logging

import functools
import redis
from aiocache import caches
from aiocache.backends.redis import RedisCache

from lidarrmetadata import config
from lidarrmetadata import cache

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)
logger.info('Have util logger')


CONFIG = config.get_config()

def _cache_config():
    """
    Builds the aiocache config for this run. PROVIDER_CACHE keeps the TheAudioDB
    and Wikipedia caches in Postgres when USE_CACHE is off. Postgres connection
    details come from the running config, so environment overrides reach them.
    """
    cache_config = copy.deepcopy(CONFIG.CACHE_CONFIG if CONFIG.USE_CACHE else CONFIG.NULL_CACHE_CONFIG)
    if not CONFIG.USE_CACHE and CONFIG.PROVIDER_CACHE:
        for name in ('tadb', 'wikipedia'):
            cache_config[name] = copy.deepcopy(CONFIG.CACHE_CONFIG[name])

    for settings in cache_config.values():
        if settings['cache'] == 'lidarrmetadata.cache.PostgresCache':
            settings.update(endpoint=CONFIG.POSTGRES_CACHE_HOST,
                            port=CONFIG.POSTGRES_CACHE_PORT,
                            user=CONFIG.POSTGRES_CACHE_USER,
                            password=CONFIG.POSTGRES_CACHE_PASSWORD,
                            db_name=CONFIG.POSTGRES_CACHE_DB)
    return cache_config

_CACHE_CONFIG = _cache_config()
caches.set_config(_CACHE_CONFIG)
logger.debug('caches: ' + ', '.join(f'{name} {settings["cache"].rsplit(".", 1)[-1]}'
                                    for name, settings in _CACHE_CONFIG.items()))

# Cache for application
CACHE = caches.get('default')
FANART_CACHE = caches.get('fanart')
TADB_CACHE = caches.get('tadb')
WIKI_CACHE = caches.get('wikipedia')
ARTIST_CACHE = caches.get('artist')
ALBUM_CACHE = caches.get('album')
SPOTIFY_CACHE = caches.get('spotify')

def first_key_item(dictionary, key, default=None):
    """
    Gets the first item from a dictionary key that returns a list
    :param dictionary: Dictionary to get item from
    :param key: Key to get
    :param default: Default value to use
    :return: First item or default
    """
    value = dictionary.get(key, default)

    if value and value != default and hasattr(value, '__getitem__'):
        return value[0]

    return value


class SentryProcessor(object):
    @abc.abstractmethod
    def _allowed(self):
        raise NotImplementedError()

    def create_event(self, event, hint):
        return event if self._allowed() else None

class SentryTtlProcessor(SentryProcessor):
    def __init__(self, ttl=1):
        """
        :param ttl: TTL in seconds
        """
        self._allowed_time = None
        self.ttl = ttl

    def _allowed(self):
        current_time = time.time()
        if self._allowed_time is None or current_time > self._allowed_time:
            self._allowed_time = current_time + self.ttl
            return True

        return False

class SentryRedisTtlProcessor(SentryProcessor):
    """
    Processor for TTL handled by redis, which should work better for multiple server processes or versions
    """
    _KEY = 'SENTRY_TTL'
    def __init__(self, redis_host='localhost', redis_port=6379, ttl=1):
        self.redis = redis.Redis(host=redis_host, port=redis_port)
        self.ttl = ttl

    def _allowed(self):
        # TODO Check on a per-exception basis
        if self.redis.exists(self._KEY):
            return False
        else:
            self.redis.set(self._KEY, "rate_limited", ex=self.ttl)

        return True
