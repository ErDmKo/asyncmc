import socket
import time
# import logging
import tornado.ioloop
import tornado.iostream
from tornado import gen
from . import constants
from . import exceptions


class Host(object):

    def __init__(self, host, conn, debug=0):
        self.debug = debug
        self.host = host
        self.port = 11211
        self.flush_on_reconnect = 1
        self.stream = None

        self.flush_on_next_connect = 0
        self.dead_retry = constants.DEAD_RETRY
        self.deaduntil = 0

        if ":" in self.host:
            parts = self.host.split(":")
            self.host = parts[0]
            self.port = int(parts[1])

        self.sock = None

    def _ensure_connection(self):
        if self.sock:
            return self

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if hasattr(s, 'settimeout'):
            s.settimeout(constants.SOCKET_TIMEOUT)
        try:
            s.connect((self.host, self.port))
        except socket.timeout as msg:
            self.mark_dead('connect: {}'.format(msg))
            return None
        except socket.error as msg:
            if isinstance(msg, tuple):
                msg = msg[1]
            self.mark_dead('connect: {}'.format(msg))
            return None
        self.sock = s
        self.stream = tornado.iostream.IOStream(s)
        self.stream.debug = True
        return self

    def _check_dead(self):
        if self.deaduntil and self.deaduntil > time.time():
            return 1
        self.deaduntil = 0
        return 0

    def mark_dead(self, reason):
        self.disconect_reason = str(reason)
        self.deaduntil = time.time() + self.dead_retry
        if self.flush_on_reconnect:
            self.flush_on_next_connect = 1
        self.close_socket()

    def close_socket(self):
        if self.sock:
            self.stream.close()
            self.sock.close()
            self.sock = None

    @gen.coroutine
    def send_cmd(self, cmd, noreply=False, stream=False):
        self._ensure_connection()
        cmd = cmd + "\r\n".encode()
        if stream:
            yield self.stream.write(cmd)
            raise gen.Return(self.stream)
        elif self.stream:
            yield self.stream.write(cmd)
        if not noreply and self.stream:
            response = yield self.stream.read_until(b'\r\n')
            raise gen.Return(response[:-2])
        if hasattr(self, 'disconect_reason'):
            raise exceptions.ConnectionDeadError(
                'socket host "{}" port "{}" disconected because "{}"'.format(
                    self.host,
                    self.port,
                    self.disconect_reason
                )
            )
