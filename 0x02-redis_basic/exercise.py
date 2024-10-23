#!/usr/bin/env python3
"""
A module for implementing a caching system using Redis NoSQL data storage.

This module provides a Cache class and supporting decorators for:
- Storing and retrieving data in Redis
- Tracking method calls and their history
- Converting data between different formats
"""
import uuid
import redis
from functools import wraps
from typing import Any, Callable, Union


def count_calls(method: Callable) -> Callable:
    """
    Decorator that tracks the number of calls made to a Cache class method.

    Args:
        method: The method to be decorated

    Returns:
        Callable: The wrapped method with call counting functionality

    Note:
        Uses Redis to maintain a persistent call count using the method's
        qualified name as the key
    """
    @wraps(method)
    def invoker(self, *args, **kwargs) -> Any:
        """
        Wrapper that increments the call counter before invoking the method.

        Args:
            *args: Positional arguments to pass to the wrapped method
            **kwargs: Keyword arguments to pass to the wrapped method

        Returns:
            Any: The result of the wrapped method
        """
        if isinstance(self._redis, redis.Redis):
            self._redis.incr(method.__qualname__)
        return method(self, *args, **kwargs)
    return invoker


def call_history(method: Callable) -> Callable:
    """
    Decorator that tracks the call details of a Cache class method.

    This decorator stores both the inputs and outputs of the wrapped method
    in Redis lists using the method's qualified name as a base for the keys.

    Args:
        method: The method to be decorated

    Returns:
        Callable: The wrapped method with call history tracking
    """
    @wraps(method)
    def invoker(self, *args, **kwargs) -> Any:
        """
        Wrapper that stores the method's inputs and outputs in Redis.

        Args:
            *args: Positional arguments to pass to the wrapped method
            **kwargs: Keyword arguments to pass to the wrapped method

        Returns:
            Any: The result of the wrapped method
        """
        in_key = '{}:inputs'.format(method.__qualname__)
        out_key = '{}:outputs'.format(method.__qualname__)
        if isinstance(self._redis, redis.Redis):
            self._redis.rpush(in_key, str(args))
        output = method(self, *args, **kwargs)
        if isinstance(self._redis, redis.Redis):
            self._redis.rpush(out_key, output)
        return output
    return invoker


def replay(fn: Callable) -> None:
    """
    Displays the call history of a Cache class method.

    This function retrieves and displays:
    - The total number of times the method was called
    - A list of all calls made to the method with their inputs and outputs

    Args:
        fn: The method whose call history should be displayed

    Note:
        The method must be decorated with @call_history to track calls
        Prints the results in the format: '{fn_name}(*{args}) -> {output}'
    """
    if fn is None or not hasattr(fn, '__self__'):
        return
    redis_store = getattr(fn.__self__, '_redis', None)
    if not isinstance(redis_store, redis.Redis):
        return
    fxn_name = fn.__qualname__
    in_key = '{}:inputs'.format(fxn_name)
    out_key = '{}:outputs'.format(fxn_name)
    fxn_call_count = 0
    if redis_store.exists(fxn_name) != 0:
        fxn_call_count = int(redis_store.get(fxn_name))
    print('{} was called {} times:'.format(fxn_name, fxn_call_count))
    fxn_inputs = redis_store.lrange(in_key, 0, -1)
    fxn_outputs = redis_store.lrange(out_key, 0, -1)
    for fxn_input, fxn_output in zip(fxn_inputs, fxn_outputs):
        print('{}(*{}) -> {}'.format(
            fxn_name,
            fxn_input.decode("utf-8"),
            fxn_output,
        ))


class Cache:
    """
    A class that provides a caching interface using Redis data storage.

    This class implements methods for:
    - Storing data of various types (str, bytes, int, float)
    - Retrieving data with optional type conversion
    - Tracking method calls and their history
    when using the appropriate decorators

    Attributes:
        _redis (redis.Redis): The Redis client instance used for storage
    """

    def __init__(self) -> None:
        """
        Initialize a new Cache instance.

        Creates a new Redis client connection and clears any existing data
        in the Redis database to ensure a clean state.
        """
        self._redis = redis.Redis()
        self._redis.flushdb(True)

    @call_history
    @count_calls
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """
        Store data in Redis and return a unique identifier.

        Args:
            data: The data to store (can be string, bytes, int, or float)

        Returns:
            str: A unique key (UUID) that can be used to retrieve the data

        Note:
            This method is decorated with @call_history and @count_calls
            to track its usage
        """
        data_key = str(uuid.uuid4())
        self._redis.set(data_key, data)
        return data_key

    def get(
            self,
            key: str,
            fn: Callable = None,
    ) -> Union[str, bytes, int, float]:
        """
        Retrieve data from Redis with optional type conversion.

        Args:
            key: The key under which the data is stored
            fn: Optional function to transform the data after retrieval

        Returns:
            The retrieved data, optionally transformed by fn if provided

        Note:
            If fn is not provided, returns the raw data as stored in Redis
        """
        data = self._redis.get(key)
        return fn(data) if fn is not None else data

    def get_str(self, key: str) -> str:
        """
        Retrieve a string value from Redis.

        Args:
            key: The key under which the string is stored

        Returns:
            str: The stored value decoded as a UTF-8 string

        Note:
            Automatically decodes bytes to UTF-8 string
        """
        return self.get(key, lambda x: x.decode('utf-8'))

    def get_int(self, key: str) -> int:
        """
        Retrieve an integer value from Redis.

        Args:
            key: The key under which the integer is stored

        Returns:
            int: The stored value converted to an integer

        Note:
            Automatically decodes bytes and converts to integer
        """
        return self.get(key, lambda x: int(x))
