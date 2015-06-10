import socket
import logging
import tornado.ioloop
import tornado.iostream
from tornado import gen
from toro import Queue, Full, Empty


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
    def send_cmd(self, cmd, *arg, **kw):
        res = yield self.hosts[hash(cmd) % len(self.hosts)] \
            .send_cmd(cmd, *arg, **kw)
        raise gen.Return(res)

    def get_stream(self, cmd, *arg, **kw):
        hosts = self.hosts[hash(cmd) % len(self.hosts)] \
            ._ensure_connection()
        return hosts.stream

    def close_socket(self):
        for host in self.hosts:
            host.close_socket()

    def get_server_for_key(self, key):
        return self.hosts[hash(key) % len(self.hosts)]


class Host(object):

    def __init__(self, host, conn, debug=0):
        self.debug = debug
        self.conn = conn
        self.host = host
        self.port = 11211
        if ":" in self.host:
            parts = self.host.split(":")
            self.host = parts[0]
            self.port = int(parts[1])

        self.sock = None

    def _ensure_connection(self):
        if self.sock:
            return self

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.host, self.port))
        except socket.error as msg:
            print(msg)
            return None
        self.sock = s
        self.stream = tornado.iostream.IOStream(s)
        self.stream.debug = True
        return self

    def close_socket(self):
        if self.sock:
            self.stream.close()
            self.sock.close()
            self.sock = None

    @gen.coroutine
    def send_cmd(self, cmd, callback=lambda: False):
        self._ensure_connection()
        cmd = cmd + "\r\n".encode()
        self.stream.write(cmd)
        response = yield self.stream.read_until(b'\r\n')
        raise gen.Return(response[:-2])
