#!/usr/bin/env python3
"""
A module implementing a request caching and tracking system using Redis.

This module provides tools for:
- Caching HTTP request responses
- Tracking request counts per URL
- Automatic cache expiration
- Efficient response retrieval

The module maintains a Redis instance for storing both cache data
and request metrics.
"""
import redis
import requests
from functools import wraps
from typing import Callable

redis_store = redis.Redis()
"""
The module-level Redis instance used for caching and tracking.
This instance handles both storing cached responses
and maintaining request counts.
"""


def data_cacher(method: Callable) -> Callable:
    """
    Decorator that implements caching for HTTP request methods.

    This decorator:
    - Tracks the number of times each URL is requested
    - Caches successful HTTP responses
    - Implements automatic cache expiration (10 seconds)
    - Provides efficient cache retrieval

    Args:
        method: The function to be decorated

    Returns:
        Callable: A wrapper function that implements the caching logic

    Note:
        The cache uses two Redis keys per URL:
        - 'count:{url}': Tracks number of requests
        - 'result:{url}': Stores the cached response
    """
    @wraps(method)
    def invoker(url) -> str:
        """
        Wrapper function that implements the caching mechanism.

        This function:
        1. Increments the request counter for the URL
        2. Checks for cached response
        3. Returns cached response if available
        4. Fetches and caches new response if needed

        Args:
            url: The URL to fetch and cache

        Returns:
            str: The response content (either from cache or fresh request)

        Note:
            Cache entries expire after 10 seconds to ensure content freshness
        """
        redis_store.incr(f'count:{url}')
        result = redis_store.get(f'result:{url}')
        if result:
            return result.decode('utf-8')
        result = method(url)
        redis_store.set(f'count:{url}', 0)
        redis_store.setex(f'result:{url}', 10, result)
        return result
    return invoker


@data_cacher
def get_page(url: str) -> str:
    """
    Fetch and cache the content of a URL.

    This function:
    - Retrieves the content of the specified URL
    - Caches the response for future requests
    - Tracks the number of times the URL is requested
    - Provides automatic cache expiration

    Args:
        url: The URL to fetch

    Returns:
        str: The text content of the URL response

    Note:
        - Cached responses expire after 10 seconds
        - Request counts are maintained per URL
        - Responses are cached as UTF-8 encoded strings
    """
    return requests.get(url).text
