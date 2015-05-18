import logging
import socket
import tornado.ioloop
import tornado.iostream
from tornado import gen
from toro import Queue, Full, Empty

from tornado.concurrent import Future


class ConnectionPool(object):

    def _info(self, msg):
        if self._debug:
            logging.debug(msg)

    def __init__(self, servers, maxsize=15, minsize=1, loop=None, debug=0):
        loop = loop if loop is not None else tornado.ioloop.IOLoop.instance()
        if debug:
            logging.basicConfig(
                level=logging.DEBUG,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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

        self._in_use.add(conn)
        return conn

    @gen.coroutine
    def _create_new_conn(self):
        conn = yield Connection.get_conn(self._servers, self._debug)
        return conn

    def reserve(self):
        conn = self.idle.popleft()
        self.in_use.append(conn)
        return conn

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

    def close_socket(self):
        for host in self.hosts:
            host.close_socket()

    def get_server_for_key(self, key):
        return self.hosts[hash(key) % len(self.hosts)]


class Host(object):

    def _info(self, msg):
        if self.debug:
            logging.info(msg)

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
            return

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.host, self.port))
        except socket.error as msg:
            print(msg)
            return None
        self.sock = s
        self.stream = tornado.iostream.IOStream(s)
        self.stream.debug = True

    def close_socket(self):
        if self.sock:
            self.stream.close()
            self.sock.close()
            self.sock = None

    def send_cmd(self, cmd, callback):
        logging.info(cmd)
        self._ensure_connection()
        cmd = cmd + "\r\n".encode()
        fut = Future()

        def close_con(*ar, **kw):
            new_fut = callback(*ar, **kw)
            self._info('con future {}'.format(new_fut))
            tornado.concurrent.chain_future(new_fut, fut)
        self.stream.write(cmd, close_con)
        fut.add_done_callback(self.close_socket)
        return fut
