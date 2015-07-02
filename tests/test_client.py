# -*- coding:utf-8 -*-
from time import sleep
from tornado import gen

from asyncmc.client import Client
from asyncmc.exceptions import ClientException, ValidationException
from ._testutil import BaseTest, run_until_complete


class FooStruct(object):

    def __init__(self):
        self.bar = "baz"

    def __str__(self):
        return "A FooStruct"

    def __eq__(self, other):
        if isinstance(other, FooStruct):
            return self.bar == other.bar
        return 0


class ConnectionCommandsTest(BaseTest):
    def setUp(self):
        super(ConnectionCommandsTest, self).setUp()
        self.mcache = Client(servers=[
            'localhost:11211'
        ], debug=1)
        self.mcache.flush_all(noreply=True)

    def tearDown(self):
        super(ConnectionCommandsTest, self).tearDown()
        self.mcache.close()

    @run_until_complete
    def test_utils(self):
        sleep(1)

    @gen.coroutine
    def check_setget(self, key, val, noreply=False):
        yield self.mcache.set(key, val, noreply=noreply)
        newval = yield self.mcache.get(key)
        self.assertEqual(newval, val)

    @run_until_complete
    def test_version(self):
        version = yield self.mcache.version()
        stats = yield self.mcache.stats()
        self.assertEqual(version, stats[b'version'])

    @run_until_complete
    def test_replace(self):
        key, value = b'key:replace', b'1'
        yield self.mcache.set(key, value)

        key_str, value_str = 'key1:replace', '1'
        yield self.mcache.set(key_str, value_str)

        key_noreply, value_noreply = 'key2:replace', '1'
        yield self.mcache.set(key_noreply, value_noreply)

        test_value = yield self.mcache.replace(key_noreply, '2', noreply=True)
        self.assertEqual(test_value, True)

        test_value = yield self.mcache.replace(key, b'2')
        self.assertEqual(test_value, True)

        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, b'2')

        test_value = yield self.mcache.replace(key_str, '2')
        self.assertEqual(test_value, True)

        test_value = yield self.mcache.get(key_str)
        self.assertEqual(test_value, '2')

        test_value = yield self.mcache.get(key_noreply)
        self.assertEqual(test_value, '2')

        test_value = yield self.mcache.replace(b'not:' + key, b'3')
        self.assertEqual(test_value, False)

        test_value = yield self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_append(self):
        key, value = b'key:append', b'1'
        yield self.mcache.set(key, value)
        values = [{
            'val': 1,
            'add': 2,
            'res': 3,
            'res1': 5
        }, {
            'val': '1',
            'add': '2',
            'res': '12',
            'res1': '122'
        }, {
            'val': [1, 2],
            'add': [3, 4],
            'res': [1, 2, 3, 4],
            'res1': [1, 2, 3, 4, 3, 4]
        }]
        for index, val in enumerate(values):
            val['key'] = 'key:append{}'.format(index)
            yield self.mcache.set(val['key'], val['val'])

        app_results = yield [
            self.mcache.append(val['key'], val['add']) for val in values
        ]

        for res in app_results:
            self.assertEqual(res, True)

        app_results = yield [
            self.mcache.get(val['key']) for val in values
        ]

        no_rep_app_results = yield [
            self.mcache.append(
                val['key'],
                val['add'],
                noreply=True
            ) for val in values
        ]

        for res in no_rep_app_results:
            self.assertEqual(res, True)

        for index, res in enumerate(app_results):
            self.assertEqual(res, values[index]['res'])

        test_value = yield self.mcache.append(key, b'2')
        self.assertEqual(test_value, True)

        # make sure value exists
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, b'12')

        test_value = yield self.mcache.append(b'not:' + key, b'3')
        self.assertEqual(test_value, False)
        # make sure value exists
        test_value = yield self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)

        app_results = yield [
            self.mcache.get(val['key']) for val in values
        ]

        for index, res in enumerate(app_results):
            self.assertEqual(res, values[index]['res1'])

    @run_until_complete
    def test_prepend(self):
        key, value = b'key:prepend', b'1'
        yield self.mcache.set(key, value)

        values = [{
            'val': 1,
            'add': 2,
            'res': 3,
            'res1': 5
        }, {
            'val': '1',
            'add': '2',
            'res': '21',
            'res1': '221'
        }, {
            'val': [1, 2],
            'add': [3, 4],
            'res': [3, 4, 1, 2],
            'res1': [3, 4, 3, 4, 1, 2]
        }]
        for index, val in enumerate(values):
            val['key'] = 'key:prepend{}'.format(index)
            yield self.mcache.set(val['key'], val['val'])

        app_results = yield [
            self.mcache.prepend(val['key'], val['add']) for val in values
        ]

        for res in app_results:
            self.assertEqual(res, True)

        app_results = yield [
            self.mcache.get(val['key']) for val in values
        ]

        for index, res in enumerate(app_results):
            self.assertEqual(res, values[index]['res'])

        no_rep_app_results = yield [
            self.mcache.prepend(
                val['key'],
                val['add'],
                noreply=True
            ) for val in values
        ]

        for res in no_rep_app_results:
            self.assertEqual(res, True)

        test_value = yield self.mcache.prepend(key, b'2')
        self.assertEqual(test_value, True)

        # make sure value exists
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, b'21')

        test_value = yield self.mcache.prepend(b'not:' + key, b'3')
        self.assertEqual(test_value, False)
        # make sure value exists
        test_value = yield self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)

        app_results = yield [
            self.mcache.get(val['key']) for val in values
        ]

        for index, res in enumerate(app_results):
            self.assertEqual(res, values[index]['res1'])

    @run_until_complete
    def test_add(self):
        key, value = b'key:add', b'1'
        yield self.mcache.set(key, value)

        key_str, value_str = 'key:add_str', '1'
        yield self.mcache.set(key_str, value_str)

        key_noreply, value_noreply = 'key:add_noreply', '1'
        yield self.mcache.set(key_noreply, value_noreply, noreply=True)

        test_value = yield self.mcache.add(key, b'2')
        self.assertEqual(test_value, False)

        test_value = yield self.mcache.add(key_str, b'2')
        self.assertEqual(test_value, False)

        test_value = yield self.mcache.add(key_noreply, b'2', noreply=True)
        # no sense here
        self.assertEqual(test_value, True)

        test_value = yield self.mcache.add('not:' + key_str, '2')
        self.assertEqual(test_value, True)

        test_value = yield self.mcache.get('not:' + key_str)
        self.assertEqual(test_value, '2')

        test_value = yield self.mcache.add(b'not:' + key, b'2')
        self.assertEqual(test_value, True)

        test_value = yield self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, b'2')

    @run_until_complete
    def test_set_typed(self):
        init_val = {
            b'array': [],
            b'dict': {'a': 'b'},
            b'init': 12313,
            'init_str': 12313,
            b'boolean': False,
            b'custom_type': set([1, 4, 4]),
            b'custom_type1': FooStruct()
        }

        results = yield [
            self.mcache.set(key, value)
            for key, value in init_val.items()
        ]

        for res in results:
            self.assertEqual(res, True)

        values = yield dict([
            (key, self.mcache.get(key)) for key, value in init_val.items()
        ])
        for key, val in init_val.items():
            self.assertEqual(val, values[key])

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

    @run_until_complete
    def test_set_expire(self):
        key, value = b'key:set', b'1'
        yield self.mcache.set(key, value, exptime=1)
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, value)
        sleep(2)
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, None)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_exeptions(self):
        f = False
        try:
            yield self.mcache.set(u'ва', 'sdfadsf')
        except ValidationException:
            f = True
        self.assertEqual(f, True)
        f = False
        try:
            yield self.mcache.multi_get('ecxeption', 'ecxeption')
        except ClientException:
            f = True
        self.assertEqual(f, True)

    @run_until_complete
    def test_set_get(self):
        key, value = b'key:set', b'1'
        yield self.mcache.set(key, value)
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, value)
        test_value = yield self.mcache.get(b'not:' + key)
        self.assertEqual(test_value, None)
        yield [
            self.check_setget(
                "a_string",
                "some random string"
            ),
            self.check_setget(
                "a_string_2",
                "some random string",
                noreply=True
            ),
            self.check_setget(
                "an_integer",
                42
            ),
            self.check_setget(
                "an_integer_2",
                42,
                noreply=True
            )
        ]

    @run_until_complete
    def test_delete(self):
        key_noreply, value_noreply = 'key:no_repleay', 'value_norep'
        yield self.check_setget(key_noreply, value_noreply)

        key, value = b'key:delete', b'value'
        yield self.check_setget(key, value)

        key_str, value_str = 'key:delete1', 'value_str'
        yield self.check_setget(key_str, value_str)

        is_deleted = yield self.mcache.delete(key_noreply, noreply=True)
        self.assertTrue(is_deleted)

        is_deleted = yield self.mcache.delete(key)
        self.assertTrue(is_deleted)

        is_deleted = yield self.mcache.delete(key_str)
        self.assertTrue(is_deleted)

        # make sure value does not exists
        test_value = yield self.mcache.get(key)
        self.assertEqual(test_value, None)

        test_value = yield self.mcache.get(key_str)
        self.assertEqual(test_value, None)

        test_value = yield self.mcache.get(key_noreply)
        self.assertEqual(test_value, None)

    @run_until_complete
    def test_str_get(self):
        key1, value1 = 'key:multi_get:1', b'1'
        key2, value2 = 'key:multi_get:2', b'2'
        yield self.mcache.set(key1, value1)
        yield self.mcache.set(key2, value2)
        test_value = yield self.mcache.multi_get(key1, key2)
        self.assertEqual(test_value, [value1, value2])

        test_value = yield self.mcache.get(key1)
        test_value = yield self.mcache.multi_get('not' + key1, key2)
        self.assertEqual(test_value, [None, value2])
        test_value = yield self.mcache.multi_get()
        self.assertEqual(test_value, [])

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
