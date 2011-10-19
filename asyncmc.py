import tornado.ioloop
import tornado.iostream
import socket
import random
import functools
import collections
import logging
import pickle
import types


class Client(object):

    _FLAG_PICKLE  = 1<<0
    _FLAG_INTEGER = 1<<1
    _FLAG_LONG    = 1<<2

    def __init__(self, servers, **kwargs):
        self.conn_pool = ConnectionPool(servers, **kwargs)

    def _debug(self, msg):
        logging.debug(msg)

    def _info(self, msg):
        logging.info(msg)

    def _server(self, key):
        return self.conn_pool.reserve().get_server_for_key(key)

    def get(self, key, callback):
        server = self._server(key)
        server.send_cmd("get %s" % (key,),
            functools.partial(self._get_callback_write, server=server, callback=callback))

    def _get_callback_write(self, server, callback):
        server.stream.read_until("\r\n",
            functools.partial(self._get_callback_read, server=server, callback=callback))

    def _get_callback_read(self, result, server, callback):
        self._debug("_get_callback_read `%s`" % (result,))
        if result[:3] == "END":
            self.conn_pool.release(server.conn)
            return callback(None)
        elif result[:5] == "VALUE":
            flag, length = result.split(" ")[2:]
            server.stream.read_until(
                "END",
                functools.partial(
                    self._get_callback_value,
                    server=server,
                    callback=callback,
                    flag=int(flag)
                )
            )
            callback(result)
        else:
            logging.error("Bad response from memcache %s" % (result,))
            self.conn_pool.release(server.conn)

    def _get_callback_value(self, result, flag, server, callback):
        result = result.replace("\r\nEND", "")
        self.conn_pool.release(server.conn)

        if flag == 0:
            value = result
        elif flag & Client._FLAG_INTEGER:
            value = int(result)
        elif flag & Client._FLAG_LONG:
            value = long(result)
        elif flag & Client._FLAG_PICKLE:
            value = pickle.loads(result)

        callback(value)

    def get_multi(self, keys, callback):
        pass

    def set(self, key, value, timeout, callback):
        assert isinstance(timeout, int)

        server = self._server(key)
        flags = 0
        if isinstance(value, types.StringTypes):
            pass
        elif isinstance(value, int):
            flags |= Client._FLAG_INTEGER
            value = "%d" % value
        elif isinstance(val, long):
            flags |= Client._FLAG_LONG
            value = "%d" % value
        else:
            flags |= Client._FLAG_PICKLE
            value = pickle.dumps(value, 2)
 
        server.send_cmd("set %s %d %d %d\r\n%s" % (key, flags, timeout, len(value), value),
            functools.partial(self._set_callback_write, server=server, callback=callback))

    def _set_callback_write(self, server, callback):
        server.stream.read_until("\r\n",
            functools.partial(self._set_callback_read, server=server, callback=callback))

    def _set_callback_read(self, result, server, callback):
        callback(result)
        self.conn_pool.release(server.conn)

    def set_multi(self, mapping, callback):
        pass

    def delete(self, key, timeout, callback):
        server = self._server(key)
        cmd = "delete %s" % (key,)
        if time:
            cmd += " %d" % (timeout,)

        server.send_cmd(
            cmd,
            functools.partial(
                self._delete_callback_write,
                callback=callback,
                server=server
            )
        )

    def _delete_callback_write(self, server, callback):
        pass

    def delete_multi(self, keys, callback):
        pass


class ConnectionPool(object):

    def __init__(self, servers, max_connections=15):
        self.pool = [Connection(servers) for i in range(max_connections)]

        self.in_use = collections.deque()
        self.idle = collections.deque(self.pool)

    def reserve(self):
        conn = self.idle.popleft()
        self.in_use.append(conn)
        return conn

    def release(self, conn):
        self.in_use.remove(conn)
        self.idle.append(conn)


class Connection(object):

    def __init__(self, servers):
        assert isinstance(servers, list)

        self.hosts = [Host(s, self) for s in servers]

    def get_server_for_key(self, key):
        return self.hosts[hash(key) % len(self.hosts)]


class Host(object):

    def __init__(self, host, conn):
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
        except socket.error, msg:
            print msg
            return None

        self.sock = s
        self.stream = tornado.iostream.IOStream(s)
        self.stream.debug=True

    def send_cmd(self, cmd, callback):
        self._ensure_connection()
        self.stream.write(cmd + "\r\n", callback)

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    c =  Client(["127.0.0.1:11211", "127.0.0.1:11212"])
    def _set_cb(res):
        print "Set callback", res

        def _get_cb(res):
            print "Get callback", res
            c.get("bar", lambda r: logging.info("get bar cb "+str(r)))

        c.get("foo", _get_cb)

    value = random.randint(1, 100)
    print "Setting value %s" % (value,)
    c.set("foo", value, 0, _set_cb)

    tornado.ioloop.IOLoop.instance().start()
