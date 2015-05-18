from functools import wraps
import tornado.ioloop
from tornado import gen
import unittest


def run_until_complete(fun):

    @wraps(fun)
    def wrapper(test, *args, **kw):
        @gen.coroutine
        def out():
            return fun(test, *args, **kw)
        test.loop._stopped = False
        test.loop.run_sync(out)
    return wrapper


class BaseTest(unittest.TestCase):

    def setUp(self):
        self.loop = tornado.ioloop.IOLoop.instance()

    def tearDown(self):
        self.loop.stop()
        del self.loop
