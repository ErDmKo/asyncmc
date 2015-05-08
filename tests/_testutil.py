from functools import wraps
import tornado.ioloop
from tornado.concurrent import Future
from tornado import gen
import unittest


def run_until_complete(fun):

    @wraps(fun)
    def wrapper(test, *args, **kw):
        fut = Future()

        @gen.coroutine
        def out():
            res = fun(test, *args, **kw)
            fut.set_result(res)
        test.loop._stopped = False
        test.loop.run_sync(out)
        return fut.result()
    return wrapper


class BaseTest(unittest.TestCase):

    def setUp(self):
        self.loop = tornado.ioloop.IOLoop.instance()

    def tearDown(self):
        self.loop.stop()
        del self.loop
