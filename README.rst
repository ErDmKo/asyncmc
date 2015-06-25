Asyncmc  
========

.. image:: https://travis-ci.org/ErDmKo/asyncmc.svg?branch=master

`Asyncmc <https://github.com/ErDmKo/asyncmc>`_ is a memcached client for `Tornado <https://github.com/tornadoweb/tornado>`_ web framework.
Asyncmc work with python 2.7 and python 3

Quick links
------------
  
* `Source (github) <https://github.com/ErDmKo/asyncmc>`_
  
* `License <https://raw.githubusercontent.com/ErDmKo/asyncmc/master/LICENSE.txt>`_
  
* `Examples <https://github.com/ErDmKo/asyncmc/blob/master/examples>`_

Hello, Memcached
-----------------


Here is a simple "Hello, Memcached" example for Tornado with Memcached.::


    import tornado.ioloop
    from tornado import gen
    import asyncmc

    loop = tornado.ioloop.IOLoop.instance()

    @gen.coroutine
    def out():
        mc = asyncmc.Client(servers=['localhost:11211'], loop=loop)
        yield mc.set(b"some_key", b"Some value")
        value = yield mc.get(b"some_key")
        print(value)
        values = yield mc.multi_get(b"some_key", b"other_key")
        print(values)
        yield mc.delete(b"another_key")

    loop.run_sync(out)

Requires
---------


+ `Tornado <https://github.com/tornadoweb/tornado>`_
+ `Memcached <http://memcached.org/>`_
 
Installation
-------------


To install asyncmc, simply:

.. code-block:: bash

    $ pip install asyncmc

LICENSE
--------

Asyncmc is licensed under MIT.
