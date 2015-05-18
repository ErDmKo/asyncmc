from time import sleep
from ._testutil import BaseTest, run_until_complete
from asyncmc.client import Client


class ConnectionCommandsTest(BaseTest):
    def setUp(self):
        super().setUp()
        self.mcache = Client(debug=1)

    def tearDown(self):
        yield self.mcache.close()
        super().tearDown()

    @run_until_complete
    def test_utils(self):
        sleep(1)
        self.assertEqual(0, 0)

    @run_until_complete
    def test_version(self):
        version = yield self.mcache.version()
        stats = yield self.mcache.stats()
        self.assertEqual(version, stats[b'version'])

    @run_until_complete
    def test_flush_all(self):
        key, value = b'key:flush_all', b'flush_all_value'
        yield self.mcache.set(key, value)
        # make sure value exists
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, value)
        # flush data
        yield self.mcache.flush_all()
        # make sure value does not exists
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, None)

        with patch.object(self.mcache, '_execute_simple_command') as patched, \
                self.assertRaises(ClientException):
            fut = asyncio.Future(loop=self.loop)
            fut.set_result(b'SERVER_ERROR error\r\n')
            patched.return_value = fut
            yield from self.mcache.flush_all()
