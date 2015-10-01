import logging
import tornado.ioloop
import socket
import binascii
from tornado import gen
from toro import Queue, Full, Empty

from .host import Host
from . import constants as const
from .exceptions import ValidationException, ConnectionDeadError


class ConnectionPool(object):

    def __init__(self, servers, maxsize=15, minsize=1, loop=None, debug=0):
        loop = loop if loop is not None else tornado.ioloop.IOLoop.instance()
        if debug:
            logging.basicConfig(
                level=logging.DEBUG,
                format="'%(levelname)s %(asctime)s"
                " %(module)s:%(lineno)d %(process)d %(thread)d %(message)s'"
            )
        self._loop = loop
        self._servers = servers
        self._minsize = minsize
        self._debug = debug
        self._in_use = set()
        self._pool = Queue(maxsize, io_loop=self._loop)

    @gen.coroutine
    def clear(self):
        """Clear pool connections."""
        while not self._pool.empty():
            conn = yield self._pool.get()
            conn.close_socket()

    def size(self):
        return len(self._in_use) + self._pool.qsize()

    @gen.coroutine
    def acquire(self):
        """Acquire connection from the pool, or spawn new one
        if pool maxsize permits.

        :return: ``Connetion`` (reader, writer)
        """
        while self.size() < self._minsize:
            _conn = yield self._create_new_conn()
            yield self._pool.put(_conn)

        conn = None
        while not conn:
            if not self._pool.empty():
                conn = yield self._pool.get()

            if conn is None:
                conn = yield self._create_new_conn()

        self._in_use.add(conn)
        raise gen.Return(conn)

    @gen.coroutine
    def _create_new_conn(self):
        conn = yield Connection.get_conn(self._servers, self._debug)
        raise gen.Return(conn)

    def release(self, conn):
        self._in_use.remove(conn)
        try:
            self._pool.put_nowait(conn)
        except (Empty, Full):
            conn.close_socket()


class Connection(object):

    def __init__(self, servers, debug=0):
        assert isinstance(servers, list)
        self.hosts = [Host(s, self, debug) for s in servers]

    @classmethod
    @gen.coroutine
    def get_conn(cls, servers, debug=0):
        return cls(servers, debug=debug)

    @gen.coroutine
    def send_cmd_all(self, cmd, *arg, **kw):
        res = []
        for host in self.hosts:
            try:
                server_resp = yield host.send_cmd(cmd, *arg, **kw)
                res.append(server_resp)
            except ConnectionDeadError as msg:
                host.mark_dead(msg)
            except socket.error as msg:
                if isinstance(msg, tuple):
                    msg = msg[1]
                host.mark_dead(msg)
        if not len(res):
            raise ConnectionDeadError(
                'no alive connetions {}'.format(
                    ', '.join(
                        map(lambda h: h.disconect_reason, self.hosts)
                    )
                )
            )
        raise gen.Return(res)

    @gen.coroutine
    def send_cmd(self, cmd, *arg, **kw):
        res = yield self.hosts[self._cmemcache_hash(cmd) % len(self.hosts)] \
            .send_cmd(cmd, *arg, **kw)
        raise gen.Return(res)

    def _cmemcache_hash(self, key):
        if isinstance(key, str):
            try:
                key = key.encode('utf-8')
            except UnicodeDecodeError as e:
                raise ValidationException('Hash exception', e)
        try:
            res = (
                (((
                    binascii.crc32(key) & 0xffffffff
                ) >> 16) & 0x7fff) or 1
            )
        except Exception as e:
            raise ValidationException('Hash exception', e)
        return res

    def _get_server(self, key):
        if isinstance(key, tuple):
            serverhash, key = key
        else:
            serverhash = self._cmemcache_hash(key)

        if not self.hosts:
            return None, None

        for i in range(const.SERVER_RETRIES):
            server = self.hosts[serverhash % len(self.hosts)]
            return server, key
        return None, None

    def get_stream(self, cmd, *arg, **kw):
        hosts = self.hosts[self._cmemcache_hash(cmd) % len(self.hosts)] \
            ._ensure_connection()
        return hosts.stream

    def close_socket(self):
        for host in self.hosts:
            host.close_socket()
