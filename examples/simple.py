import tornado.ioloop
from tornado import gen
import asyncmc

i_loop = tornado.ioloop.IOLoop.instance()


@gen.coroutine
def out():
    mc = asyncmc.Client(servers=['localhost:11211'], loop=i_loop)
    yield mc.set(b"some_key", b"Some value")
    value = yield mc.get(b"some_key")
    print(value)
    values = yield mc.multi_get(b"some_key", b"other_key")
    print(values)
    yield mc.delete(b"another_key")

i_loop.run_sync(out)
