import socket
import logging
import tornado.ioloop
import tornado.iostream
from tornado import gen


class Host(object):

    def __init__(self, host, conn, debug=0):
        self.debug = debug
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
            logging.error(msg)
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
    def send_cmd(self, cmd, noreply=False, stream=False):
        self._ensure_connection()
        cmd = cmd + "\r\n".encode()
        '''
        logging.info(cmd)
        import pdb; pdb.set_trace()
        '''
        if stream:
            yield self.stream.write(cmd)
            raise gen.Return(self.stream)
        else:
            yield self.stream.write(cmd)
        if not noreply:
            response = yield self.stream.read_until(b'\r\n')
            raise gen.Return(response[:-2])
