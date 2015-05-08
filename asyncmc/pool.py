import logging
import socket
import collections
import tornado.ioloop
import tornado.iostream

from tornado.concurrent import Future


class ConnectionPool(object):

    def __init__(self, servers, max_connections=15, debug=0):
        self.pool = [
            Connection(servers, debug)
            for i in range(max_connections)
        ]

        self.in_use = collections.deque()
        self.idle = collections.deque(self.pool)

    def size(self):
        return len(self.pool)

    def reserve(self):
        conn = self.idle.popleft()
        self.in_use.append(conn)
        return conn

    def release(self, conn):
        self.in_use.remove(conn)
        self.idle.append(conn)


class Connection(object):

    def __init__(self, servers, debug=0):
        assert isinstance(servers, list)

        self.hosts = [Host(s, self, debug) for s in servers]

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

    def close_socket(self, resp):
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
