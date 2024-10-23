#!/usr/bin/env python3
"""
A module providing a robust request caching and tracking system using Redis.

This module implements a caching system that:
- Caches HTTP request responses
- Tracks request counts
- Handles cache expiration
- Provides type-safe operations
"""
from __future__ import annotations

import redis
import requests
from functools import wraps
from typing import Callable, Optional, TypeVar, cast
from dataclasses import dataclass
from datetime import timedelta

T = TypeVar('T')


@dataclass
class CacheConfig:
    """Configuration settings for the caching system."""
    expiration_time: int = 10  # seconds
    prefix_count: str = 'count'
    prefix_result: str = 'result'


class RequestCache:
    """Handles caching operations for HTTP requests."""

    def __init__(
        self,
        redis_client: Optional[redis.Redis] = None,
        config: Optional[CacheConfig] = None
    ) -> None:
        """
        Initialize the RequestCache with a Redis client and configuration.

        Args:
            redis_client: Redis client instance. Creates new if None.
            config: Cache configuration settings. Uses defaults if None.
        """
        self.redis = redis_client or redis.Redis()
        self.config = config or CacheConfig()

    def _make_key(self, prefix: str, url: str) -> str:
        """
        Create a Redis key with proper formatting.

        Args:
            prefix: Key prefix (e.g., 'count' or 'result')
            url: URL string to be cached

        Returns:
            Formatted Redis key
        """
        return f"{prefix}:{url}"

    def increment_count(self, url: str) -> int:
        """
        Increment the request counter for a URL.

        Args:
            url: The URL being requested

        Returns:
            New count value
        """
        key = self._make_key(self.config.prefix_count, url)
        return cast(int, self.redis.incr(key))

    def get_cached_result(self, url: str) -> Optional[str]:
        """
        Retrieve cached result for a URL.

        Args:
            url: The URL to lookup

        Returns:
            Cached content if available, None otherwise
        """
        key = self._make_key(self.config.prefix_result, url)
        result = self.redis.get(key)
        return result.decode('utf-8') if result else None

    def cache_result(self, url: str, content: str) -> None:
        """
        Cache the result for a URL with expiration.

        Args:
            url: The URL being cached
            content: The content to cache
        """
        count_key = self._make_key(self.config.prefix_count, url)
        result_key = self._make_key(self.config.prefix_result, url)

        # Reset count and store result
        self.redis.set(count_key, 0)
        self.redis.setex(
            result_key,
            timedelta(seconds=self.config.expiration_time),
            content
        )


def create_cache_decorator(
    cache: RequestCache
) -> Callable[[Callable[[str], str]], Callable[[str], str]]:
    """
    Create a decorator for caching URL request results.

    Args:
        cache: RequestCache instance to use for caching

    Returns:
        Decorator function for caching URL requests
    """
    def data_cacher(method: Callable[[str], str]) -> Callable[[str], str]:
        """
        Decorator that caches the output of fetched data.

        Args:
            method: The function to wrap (should accept URL and return content)

        Returns:
            Wrapped function that implements caching
        """
        @wraps(method)
        def wrapper(url: str) -> str:
            """
            Wrapper function that handles caching logic.

            Args:
                url: The URL to fetch and cache

            Returns:
                Content from cache or fresh request
            """
            # Increment request counter
            cache.increment_count(url)

            # Try to get cached result
            cached_result = cache.get_cached_result(url)
            if cached_result is not None:
                return cached_result

            # Fetch and cache new result
            result = method(url)
            cache.cache_result(url, result)
            return result

        return wrapper
    return data_cacher


# Initialize default cache instance
default_cache = RequestCache()

# Create decorator using default cache
data_cacher = create_cache_decorator(default_cache)


@data_cacher
def get_page(url: str) -> str:
    """
    Fetch and return the content of a URL, with caching and request tracking.

    Args:
        url: The URL to fetch

    Returns:
        The text content of the URL response

    Raises:
        requests.RequestException: If the request fails
    """
    response = requests.get(url)
    response.raise_for_status()  # Raise exception for bad status codes
    return response.text
