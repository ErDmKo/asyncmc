from time import sleep
from ._testutil import run_until_complete, BaseTest
from asyncmc.pool import ConnectionPool


class PoolTest(BaseTest):

    @run_until_complete
    def test_utils(self):
        sleep(1)
        self.assertEqual(0, 0)

    @run_until_complete
    def test_pool_creation(self):
        pool = ConnectionPool(['memcached:11211'], debug=1)
        self.assertEqual(pool.size(), 0)
        self.assertEqual(pool._minsize, 1)
