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
            parts = self.host.rsplit(":", 1)
            self.host = parts[0]
            self.port = int(parts[1])
        if self.host.startswith('[') and self.host.endswith(']'):
            self.host = self.host[1:-1]

        self.sock = None

    def _ensure_connection(self):
        if self.sock:
            return self

        remaining = constants.SOCKET_TIMEOUT
        last_error = None

        for family, socktype, proto, _, addr in socket.getaddrinfo(
            self.host, self.port, socket.AF_UNSPEC, socket.SOCK_STREAM
        ):
            if not remaining:
                self.mark_dead('connect: no time left')
                return None
            try:
                s = socket.socket(family, socktype, proto)
                s.settimeout(remaining)
                start = time.time()
                s.connect(addr)
                break
            except socket.timeout as msg:
                self.mark_dead('connect: {}'.format(msg))
                return None
            except socket.error as msg:
                if isinstance(msg, tuple):
                    msg = msg[1]
                last_error = msg
                s.close()
                duration = time.time() - start
                remaining = max(remaining - duration, 0)
        else:
            # if we never broke out of the getaddr loop
            self.mark_dead('connect: {}'.format(last_error))
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
