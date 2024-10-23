![](https://s3.amazonaws.com/alx-intranet.hbtn.io/uploads/medias/2020/1/40eab4627f1bea7dfe5e.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARDDGGGOUSBVO6H7D%2F20241023%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20241023T212018Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=9851943c977b8f75e5ede8993d3551da9ce91f6f69be39b714797df6eb15c70b)

Resources
---------

**Read or watch:**

*   [Redis Crash Course Tutorial](/rltoken/hJVo3XwMMFFoApyX8zPXvA "Redis Crash Course Tutorial")
*   [Redis commands](/rltoken/oauvbRmxM12SxvimzqhrOg "Redis commands")
*   [Redis python client](/rltoken/imfgFhAZPlg7YMZ_tHvFZw "Redis python client")
*   [How to Use Redis With Python](/rltoken/7SluvFvgckwVgsvrfOf1CQ "How to Use Redis With Python")

Learning Objectives
-------------------

*   Learn how to use redis for basic operations
*   Learn how to use redis as a simple cache

Requirements
------------

*   All of your files will be interpreted/compiled on Ubuntu 18.04 LTS using python3 (version 3.7)
*   All of your files should end with a new line
*   A `README.md` file, at the root of the folder of the project, is mandatory
*   The first line of all your files should be exactly `#!/usr/bin/env python3`
*   Your code should use the `pycodestyle` style (version 2.5)
*   All your modules should have documentation (`python3 -c 'print(__import__("my_module").__doc__)'`)
*   All your classes should have documentation (`python3 -c 'print(__import__("my_module").MyClass.__doc__)'`)
*   All your functions and methods should have documentation (`python3 -c 'print(__import__("my_module").my_function.__doc__)'` and `python3 -c 'print(__import__("my_module").MyClass.my_function.__doc__)'`)
*   A documentation is not a simple word, it’s a real sentence explaining what’s the purpose of the module, class or method (the length of it will be verified)
*   All your functions and coroutines must be type-annotated.

Install Redis on Ubuntu 18.04
-----------------------------

    $ sudo apt-get -y install redis-server
    $ pip3 install redis
    $ sed -i "s/bind .*/bind 127.0.0.1/g" /etc/redis/redis.conf
    

Use Redis in a container
------------------------

Redis server is stopped by default - when you are starting a container, you should start it with: `service redis-server start`
