#!/usr/bin/env python3
"""
A module for using Redis NoSQL data storage
with improved organization and type safety.
"""
from __future__ import annotations
import uuid
import redis
from functools import wraps
from typing import Any, Callable, Union, TypeVar, cast, Optional

T = TypeVar('T')
CacheData = Union[str, bytes, int, float]
RedisClient = redis.Redis


def create_key(prefix: str, name: str) -> str:
    """Create a Redis key with proper formatting."""
    return f"{prefix}:{name}"


class RedisTracker:
    """Handles Redis tracking operations for method calls."""

    @staticmethod
    def increment_counter(redis_client: RedisClient, method_name: str) -> None:
        """Increment the call counter for a method."""
        redis_client.incr(method_name)

    @staticmethod
    def store_call_history(
        redis_client: RedisClient,
        method_name: str,
        args: tuple,
        output: Any
    ) -> None:
        """Store method call inputs and outputs."""
        in_key = create_key(method_name, 'inputs')
        out_key = create_key(method_name, 'outputs')
        redis_client.rpush(in_key, str(args))
        redis_client.rpush(out_key, str(output))

    @staticmethod
    def get_call_history(redis_client: RedisClient,
                         method_name: str) -> tuple[list, list]:
        """Retrieve method call history."""
        in_key = create_key(method_name, 'inputs')
        out_key = create_key(method_name, 'outputs')
        return (
            redis_client.lrange(in_key, 0, -1),
            redis_client.lrange(out_key, 0, -1)
        )


def count_calls(method: Callable[..., T]) -> Callable[..., T]:
    """Decorator to track the number of calls made to a method."""
    @wraps(method)
    def wrapper(self: Cache, *args: Any, **kwargs: Any) -> T:
        if isinstance(self._redis, redis.Redis):
            RedisTracker.increment_counter(self._redis, method.__qualname__)
        return method(self, *args, **kwargs)
    return wrapper


def call_history(method: Callable[..., T]) -> Callable[..., T]:
    """Decorator to track method call history."""
    @wraps(method)
    def wrapper(self: Cache, *args: Any, **kwargs: Any) -> T:
        output = method(self, *args, **kwargs)
        if isinstance(self._redis, redis.Redis):
            RedisTracker.store_call_history(
                self._redis,
                method.__qualname__,
                args,
                output
            )
        return output
    return wrapper


def replay(fn: Optional[Callable]) -> None:
    """Display the call history of a Cache class method."""
    if fn is None or not hasattr(fn, '__self__'):
        return

    cache_instance = cast(Cache, fn.__self__)
    if not isinstance(cache_instance._redis, redis.Redis):
        return

    method_name = fn.__qualname__
    call_count = cache_instance._redis.get(method_name)
    if call_count is None:
        return

    print(f"{method_name} was called {int(call_count)} times:")

    inputs, outputs = RedisTracker.get_call_history(
        cache_instance._redis, method_name)
    for input_data, output_data in zip(inputs, outputs):
        print(f"{method_name}(*{input_data.decode('utf-8')
                                }) -> {output_data.decode('utf-8')}")


class Cache:
    """A class for storing and retrieving data in Redis
    with tracking capabilities."""

    def __init__(self) -> None:
        """Initialize Redis connection and clear existing data."""
        self._redis: RedisClient = redis.Redis()
        self._redis.flushdb(True)

    @call_history
    @count_calls
    def store(self, data: CacheData) -> str:
        """Store data in Redis with a unique key."""
        key = str(uuid.uuid4())
        self._redis.set(key, str(data))
        return key

    def get(
        self,
        key: str,
        fn: Optional[Callable[[bytes], T]] = None
    ) -> Union[bytes, T]:
        """
        Retrieve data from Redis with optional transformation.

        Args:
            key: Redis key to retrieve
            fn: Optional function to transform the retrieved data

        Returns:
            Retrieved data, transformed if fn is provided
        """
        data = self._redis.get(key)
        if data is None:
            raise KeyError(f"No data found for key: {key}")
        return fn(data) if fn else data

    def get_str(self, key: str) -> str:
        """Retrieve string data from Redis."""
        return cast(str, self.get(key, lambda x: x.decode('utf-8')))

    def get_int(self, key: str) -> int:
        """Retrieve integer data from Redis."""
        return cast(int, self.get(key, lambda x: int(x.decode('utf-8'))))
