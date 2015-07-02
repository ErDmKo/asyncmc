import tornado.ioloop
import functools
import pickle
import json
import re
import logging
from tornado import gen

from . import constants as const
from .exceptions import ClientException, ValidationException
from .pool import ConnectionPool

"""client module for memcached (memory cache daemon)

Overview
========

See U{the MemCached homepage<http://www.danga.com/memcached>} for more
about memcached.

Usage summary
=============

This should give you a feel for how this module operates::

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

The standard way to use memcache with a database is like this:

    obj = yield mc.get(key)
    if not obj:
        obj = backend_api.get(...)
        yield mc.set(key, obj)

    # we now have obj, and future passes through this code
    # will use the object from the cache.

Detailed Documentation
======================

More detailed documentation is available in the L{Client} class.

"""


def acquire(func):

    @gen.coroutine
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        conn = yield self.pool.acquire()
        try:
            res = yield func(self, conn, *args, **kwargs)
            raise gen.Return(res)
        finally:
            self.pool.release(conn)

    return wrapper


class Client(object):
    """Object representing a memcache server.

    See L{memcache} for an overview.
    In any case key will be a simple hashable type (string, integer, etc.)


    @group Insertion: set, add, replace
    @group Retrieval: get, get_multi
    @group Removal: delete

    """

    def __init__(self, **kwargs):
        self.debug = kwargs.get('debug')
        self.io_loop = kwargs.get('loop', tornado.ioloop.IOLoop.instance())
        self.pool = ConnectionPool(
            kwargs.get('servers', ["localhost:11211"]),
            debug=self.debug,
            loop=self.io_loop,
            minsize=kwargs.get('pool_minsize', 1),
            maxsize=kwargs.get('pool_size', 15)
        )

        """Create a new Client object with the given list of servers.
            @param servers: C{servers} is passed to L{set_servers}.
            @param loop: Event loop which is used for ansync operations.
                It is optional but if is not defined it will be tornado
                singletone event loop instance.
            @param pool_minsize: Minimal number of connetions with memcashed
                server
            @param pool_size: Maximal number of connetions with memcashed
                server
        """

    # key supports ascii sans space and control chars
    # \x21 is !, right after space, and \x7e is -, right before DEL
    # also 1 <= len <= 250 as per the spec
    _valid_key_re = re.compile(b'^[\x21-\x7e]{1,250}$')

    @acquire
    @gen.coroutine
    def stats(self, conn, args=None):
        """Runs a stats command on the server

        @param args: Additional arguments to pass to the memcache
            "stats" command.
        """
        # req  - stats [additional args]\r\n
        # resp - STAT <name> <value>\r\n (one per result)
        #        END\r\n
        if args is None:
            args = b''
        cmd = b''.join((b'stats ', args))
        resp = yield conn.send_cmd(cmd)
        result = {}
        while resp != b'END\r\n':
            terms = resp.split()

            if len(terms) == 2 and terms[0] == b'STAT':
                result[terms[1]] = None
            elif len(terms) == 3 and terms[0] == b'STAT':
                result[terms[1]] = terms[2]
            else:
                raise ClientException('stats failed', resp)

            resp = yield conn.get_stream(cmd).read_until(b'\r\n')

        raise gen.Return(result)

    @acquire
    @gen.coroutine
    def version(self, conn):
        """Current version of the server.

        @return: bytes, memcached version for current the server.
        """
        command = b'version'
        response = yield conn.send_cmd(command)
        if not response.startswith(const.VERSION):
            raise ClientException('Memcached version failed', response)
        version, number = response.split()
        raise gen.Return(number)

    def _key_type(self, key_list=[], key=None):
        out_keys = []

        if not key_list and key:
            if not isinstance(key, bytes):
                key = key.encode('utf-8')
            return key

        for key in key_list:
            if not isinstance(key, bytes):
                key = key.encode('utf-8')
            out_keys.append(key)
        return out_keys

    def close(self):
        self.pool.clear()

    def _value_type(self, value):
        flag = 0
        if isinstance(value, bytes):
            pass
        elif isinstance(value, str):
            flag |= const.FLAG_STRING
            value = value.encode('utf-8')
        elif isinstance(value, bool):
            flag |= const.FLAG_BOOLEAN
            value = str(int(value)).encode('utf-8')
        elif isinstance(value, int):
            flag |= const.FLAG_INTEGER
            value = str(value).encode('utf-8')
        else:
            try:
                value = json.dumps(value).encode('utf-8')
                flag |= const.FLAG_JSON
            except Exception as e:
                logging.info(e)
                value = pickle.dumps(value, 2)
                flag |= const.FLAG_PICKLE

        if not isinstance(value, bytes):
            logging.info(value)
            value = bytes(value)

        return value, flag

    @acquire
    @gen.coroutine
    def multi_get(self, conn, *keys):
        """Retrieves multiple keys from the memcache doing just one query.

        This method is recommended over regular L{get} as it lowers
        the number of total packets flying around your network,
        reducing total latency, since your app doesn't have to wait
        for each round-trip of L{get} before sending the next one.

        @param keys: list keys for the item being fetched.
        @return: list of values for the specified keys.
        @raises: ValidationException, ClientException,
            and socket errors
        """
        result = yield self._multi_get(conn, *self._key_type(key_list=keys))
        raise gen.Return(result)

    @acquire
    @gen.coroutine
    def flush_all(self, conn, noreply=False):
        """Its effect is to invalidate all existing items immediately"""
        command = b'flush_all' + (b' noreply' if noreply else b'')
        response = yield conn.send_cmd_all(command)

        if [const.OK for n in range(len(response))] != response:
            raise ClientException('Memcached flush_all failed', response)

    @acquire
    @gen.coroutine
    def get(self, conn, key, default=None):
        """Gets a single value from the server.

        @param key: bytes or string, is the key for the item being fetched
        @param default: default value if there is no value.
            #DOTO test default value
        @return: custom type, is the data for this specified key.
        """
        result = yield self._multi_get(conn, self._key_type(key=key))
        result = result[0] if result else default
        raise gen.Return(result)

    @acquire
    @gen.coroutine
    def set(self, conn, key, value, exptime=0, noreply=False):
        """Sets a key to a value on the server
        with an optional exptime (0 means don't auto-expire)

        @param key: bytes or string, is the key of the item.
        @param value: custom type, data to store.
        @param exptime: Tells memcached the time which this value should
            expire, either as a delta number of seconds, or an absolute
            unix time-since-the-epoch value. See the memcached protocol
            docs section "Storage Commands" for more info on <exptime>. We
            default to 0 == cache forever.
        @param noreply: optional parameter instructs the server to not
        send the reply.

        @return: bool, True in case of success.
        """
        resp = yield self._storage_command(
            conn, b'set', self._key_type(key=key), value, exptime, noreply)
        raise gen.Return(resp)

    @gen.coroutine
    def _multi_get(self, conn, *keys):
        # req  - get <key> [<key> ...]\r\n
        # resp - VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        #        <data block>\r\n (if exists)
        #        [...]
        #        END\r\n
        if not keys:
            raise gen.Return([])

        [self._validate_key(key) for key in keys]
        if len(set(keys)) != len(keys):
            raise ClientException('duplicate keys passed to multi_get')
        cmd = b'get ' + b' '.join(keys)
        servers_resp = yield conn.send_cmd_all(cmd, stream=True)
        received = {}
        for stream in servers_resp:
            line = yield stream.read_until(b'\n')
            while line != b'END\r\n':
                terms = line.split()

                if len(terms) == 4 and terms[0] == b'VALUE':  # exists
                    key = terms[1]
                    flags = int(terms[2])
                    length = int(terms[3])

                    val = yield stream.read_bytes(length+2)
                    val = val[:-2]

                    if flags == 0:
                        pass
                    elif flags & const.FLAG_STRING:
                        val = val.decode('utf-8')
                    elif flags & const.FLAG_BOOLEAN:
                        val = bool(int(val))
                    elif flags & const.FLAG_INTEGER:
                        val = int(val)
                    elif flags & const.FLAG_JSON:
                        val = json.loads(val.decode('utf-8'))
                    elif flags & const.FLAG_PICKLE:
                        val = pickle.loads(val)
                    else:
                        val = False

                    if val is False and not flags & const.FLAG_BOOLEAN:
                        raise ClientException('Unknown flag from server')
                    if key in received:
                        raise ClientException('duplicate results from servers')

                    received[key] = val
                else:
                    raise ClientException('get{} failed'.format(cmd), line)
                line = yield stream.read_until(b'\n')

        if len(received) > len(keys):
            raise ClientException('received too many responses')
        res = [received.get(k, None) for k in keys]
        raise gen.Return(res)

    @acquire
    @gen.coroutine
    def replace(self, conn, key, value, exptime=0, noreply=False):
        """Store this data, but only if the server *does*
            already hold data for this key.

        @param key: bytes or string, is the key of the item.
        @param value: custom class,  data to store.
        @param exptime: int is expiration time. If it's 0, the
            item never expires.
        @return: bool, True in case of success.
        """
        res = yield self._storage_command(
            conn, b'replace', self._key_type(key=key), value, exptime, noreply)
        raise gen.Return(res)

    @acquire
    @gen.coroutine
    def append(self, conn, key, value, exptime=0, noreply=False):
        """Add data to an existing key after existing data

        Also see L{prepend}.

        @param key: bytes or string, is the key of the item.
        @param value: custom type, data to store.
        @param exptime: int is expiration time. If it's 0, the
            item never expires.
        @return: bool, True in case of success.
        """
        if isinstance(value, bytes) or isinstance(value, str):
            command = b'append'
        else:
            command = b'set'
            old_val = yield self.get(key)
            value = old_val + value

        res = yield self._storage_command(
            conn, command, self._key_type(key=key), value, exptime, noreply)
        raise gen.Return(res)

    @acquire
    @gen.coroutine
    def prepend(self, conn, key, value, exptime=0, noreply=False):
        """Add data to an existing key before existing data

        @param key: bytes or string, is the key of the item.
        @param value: custom type, data to store.
        @param exptime: int is expiration time. If it's 0, the
            item never expires.
        @return: bool, True in case of success.
        """
        if isinstance(value, bytes) or isinstance(value, str):
            command = b'prepend'
        else:
            command = b'set'
            old_val = yield self.get(key)
            value = value + old_val

        res = yield self._storage_command(
            conn, command, self._key_type(key=key), value, exptime, noreply)
        raise gen.Return(res)

    @acquire
    @gen.coroutine
    def add(self, conn, key, value, exptime=0, noreply=False):
        """Store this data, but only if the server *doesn't* already
        hold data for this key.

        @param noreply: optional parameter instructs the server to not send the
            reply.

        @param value: srings or bytes, data to store.
        @param exptime: int is expiration time. If it's 0, the
            item never expires.
        @return: bool, True in case of success.
        """
        if noreply:
            logging.warning('Call add method with noreply tag')

        res = yield self._storage_command(
            conn, b'add', self._key_type(key=key), value, exptime, noreply)
        raise gen.Return(res)

    @acquire
    @gen.coroutine
    def delete(self, conn, key, noreply=False):
        """Deletes a key/value pair from the server.

        @return: Nonzero on success. True if case values was
             deleted or False to indicate
        that the item with this key was not found.
  .
        @param noreply: optional parameter instructs the server to not send the
            reply.

        """
        server, key = conn._get_server(key)

        key = self._key_type(key=key)
        assert self._validate_key(key)

        command = b'delete ' + key + (b' noreply' if noreply else b'')
        response = yield server.send_cmd(command, noreply)

        if not noreply and response not in (const.DELETED, const.NOT_FOUND):
            raise ClientException('Memcached delete failed', response)
        raise gen.Return(response == const.DELETED or noreply)

    @gen.coroutine
    def _storage_command(self, conn, command, key, value,
                         exptime=0, noreply=False):
        # req  - set <key> <flags> <exptime> <bytes> [noreply]\r\n
        #        <data block>\r\n
        # resp - STORED\r\n (or others)

        # typically, if val is > 1024**2 bytes server returns:
        #   SERVER_ERROR object too large for cache\r\n
        # however custom-compiled memcached can have different limit
        # so, we'll let the server decide what's too much
        server, key = conn._get_server(key)

        assert self._validate_key(key)

        if not isinstance(exptime, int) or isinstance(exptime, bool):
            raise ValidationException('exptime not int', exptime)
        elif exptime < 0:
            raise ValidationException('exptime negative', exptime)

        value, flags = self._value_type(value)

        args_arr = [flags, exptime, len(value)]
        if noreply:
            args_arr.append('noreply')
        args = [str(a).encode('utf-8') for a in args_arr]
        _cmd = b' '.join([command, key] + args) + b'\r\n'
        cmd = _cmd + value
        server, key = conn._get_server(key)

        resp = yield server.send_cmd(cmd, noreply=noreply)

        if not noreply and resp not in (const.STORED, const.NOT_STORED):
            raise ClientException('stats "{}" failed'.format(cmd), resp)
        raise gen.Return(resp == const.STORED or noreply)

    def _validate_key(self, key):
        if not isinstance(key, bytes):  # avoid bugs subtle and otherwise
            raise ValidationException('key must be bytes', key)

        m = self._valid_key_re.match(key)
        if m:
            # in python re, $ matches either end of line or right before
            # \n at end of line. We can't allow latter case, so
            # making sure length matches is simplest way to detect
            if len(m.group(0)) != len(key):
                raise ValidationException('trailing newline', key)
        else:
            raise ValidationException('invalid key', key)

        return key
