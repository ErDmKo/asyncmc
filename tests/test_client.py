from time import sleep
import logging
from collections import defaultdict

from asyncmc.client import Client
from ._testutil import BaseTest, run_until_complete


class ConnectionCommandsTest(BaseTest):
    def setUp(self):
        super(ConnectionCommandsTest, self).setUp()
        self.mcache = Client(debug=1)

    def tearDown(self):
        yield self.mcache.close()
        super(ConnectionCommandsTest, self).tearDown()

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
    def test_set_typed(self):
        value = {
            b'array': [],
            b'dict': {'a': 'b'},
            b'init': 12313,
            b'boolean': False,
            b'custom_type': set([1, 4, 4])
        }
        yield [self.mcache.set(key, value) for key, value in value.items()]
        values = yield dict([
            (key, self.mcache.get(key)) for key, value in value.items()
        ])
        for key, val in value.items():
            self.assertEqual(val, values[key])

    @run_until_complete
    def test_flush_all(self):
        key, value = b'key:flush_all', b'flush_all_value'
        yield self.mcache.set(key, value)
        # make sure value exists
        test_value = yield self.mcache.get(key)
        logging.info(test_value)
        self.assertEqual(test_value, value)
        # flush data
        yield self.mcache.flush_all()
        # make sure value does not exists
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_set_get(self):
        key, value = b'key:set', b'1'
        yield self.mcache.set(key, value)
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, value)
        test_value = yield self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_multi_get(self):
        key1, value1 = b'key:multi_get:1', b'1'
        key2, value2 = b'key:multi_get:2', b'2'
        yield self.mcache.set(key1, value1)
        yield self.mcache.set(key2, value2)
        test_value = yield self.mcache.multi_get(key1, key2)
        self.assertEqual(test_value, [value1, value2])

        test_value = yield self.mcache.multi_get(b'not' + key1, key2)
        self.assertEqual(test_value, [None, value2])
        test_value = yield self.mcache.multi_get()
        self.assertEqual(test_value, [])
