import tornado.ioloop
import random
import functools
import logging
import pickle
import time
from tornado.concurrent import Future
from .pool import ConnectionPool


class Client(object):

    _FLAG_PICKLE = 1 << 0
    _FLAG_INTEGER = 1 << 1
    _FLAG_LONG = 1 << 2

    def __init__(self, servers=["memcached:11211"], debug=0, **kwargs):
        self.debug = debug
        self.io_loop = tornado.ioloop.IOLoop.instance()
        self.conn_pool = ConnectionPool(servers, debug=debug, **kwargs)

    def _debug(self, msg):
        if self.debug:
            logging.debug(msg)

    def _info(self, msg):
        if self.debug:
            logging.info(msg)

    def _server(self, key):
        return self.conn_pool.reserve().get_server_for_key(key)

    def get(self, key, callback=lambda rez: rez):
        server = self._server(key)
        in_callback = functools.partial(
            self._get_callback_write,
            server=server,
            callback=callback
        )
        self._info('in get')
        return server.send_cmd("get {}".format(key).encode(), in_callback)

    def _get_callback_write(self, server, callback):
        in_callback = functools.partial(
            self._get_callback_read,
            server=server,
            callback=callback
        )
        self._info('in get callback')
        fut = Future()

        def con_close(*ar, **kw):
            new_fut = in_callback(*ar, **kw)
            tornado.concurrent.chain_future(new_fut, fut)
            self._info('get result call')
        server.stream.read_until(b"\r\n", con_close)
        return fut

    def _get_callback_read(self, result, server, callback):
        self._info("_get_callback_read `%s`" % (result,))
        if result[:3] == b"END":
            self.conn_pool.release(server.conn)
            fut = Future()
            fut.set_result(callback(None))
            return fut
        elif result[:5] == b"VALUE":
            flag, length = result.split(b" ")[2:]
            in_callback = functools.partial(
                self._get_callback_value,
                server=server,
                callback=callback,
                flag=int(flag)
            )
            fut = Future()

            def con_close(*ar, **kw):
                self._info('get result call {}'.format(result))
                info = in_callback(*ar, **kw)
                fut.set_result(info)
                return info
            server.stream.read_until(
                b"END", con_close)
            return fut
        else:
            logging.error("Bad response from  memcache >%s<" % (result,))
            self.conn_pool.release(server.conn)
            raise Exception('Bad resp memcached')

    def _get_callback_value(self, result, flag, server, callback):
        result = result.replace(b"\r\nEND", b"")
        self.conn_pool.release(server.conn)

        if flag == 0:
            value = result
        elif flag & Client._FLAG_INTEGER:
            value = int(result)
        # elif flag & Client._FLAG_LONG:
        #    value = long(result)
        elif flag & Client._FLAG_PICKLE:
            value = pickle.loads(result)
        callback(value)
        return value

    def get_multi(self, keys, callback):
        pass

    def set(self, key, value, timeout=0, callback=lambda rez: rez):
        assert isinstance(timeout, int)
        self._info('insert key {}'.format(key))

        server = self._server(key)
        flags = 0
        if isinstance(value, str):
            pass
        elif isinstance(value, int):
            flags |= Client._FLAG_INTEGER
            value = str(value).encode()
        else:
            flags |= Client._FLAG_PICKLE
            value = pickle.dumps(value, 2)
        str_info = {
            'key': key,
            'flags': flags,
            'timeout': timeout,
            'length': len(value),
            }
        in_callback = functools.partial(
            self._set_callback_write,
            server=server,
            callback=callback
        )
        return server.send_cmd(
            "set {key} {flags} {timeout}{length: d}\r\n"
            .format(**str_info)
            .encode()+value,
            in_callback
        )

    def _set_callback_write(self, server, callback):
        in_callback = functools.partial(
            self._set_callback_read,
            server=server,
            callback=callback
        )
        fut = Future()

        def close_con(*ar, **kw):
            new_fut = in_callback(*ar, **kw)
            tornado.concurrent.chain_future(new_fut, fut)
            self._info('set result call')
        server.stream.read_until(b"\r\n", close_con)
        return fut

    def _set_callback_read(self, result, server, callback):
        self._info('read {}'.format(result))
        self.conn_pool.release(server.conn)
        fut = Future()
        fut.set_result(callback(result))
        return fut

    def set_multi(self, mapping, callback):
        pass

    def delete(self, key, timeout=0, callback=lambda r: r):
        server = self._server(key)
        cmd = "delete %s" % (key,)
        if timeout:
            cmd += " %d" % (timeout,)

        in_callback = functools.partial(
                self._delete_callback_write,
                callback=callback,
                server=server)
        return server.send_cmd(cmd.encode(), in_callback)

    def _delete_callback_write(self, server, callback):
        in_callback = functools.partial(
            self._set_callback_read,
            server=server,
            callback=callback
        )
        fut = Future()

        def close_con(*ar, **kw):
            new_fut = in_callback(*ar, **kw)
            tornado.concurrent.chain_future(new_fut, fut)
            self._info('set result call')
        server.stream.read_until(b"\r\n", close_con)
        return fut

    def delete_multi(self, keys, callback):
        pass

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    value = random.randint(1, 100)
    server = tornado.ioloop.IOLoop.instance()
    c = Client(["127.0.0.1:11211"], debug=1)

    def _set_cb(res):
        print("\n\nSet callback", res)

        def _get_cb(res):
            print("\n\n\nGet callback!", res)
            c.get("bar", lambda r: logging.info("\n\n\n\nget bar "+str(r)))
            c.delete('foo', 0, lambda r: c.get('foo', lambda r1: logging.info('\n\n\n\nget_deleted \
                {} info {}'.format(r1, r))))

        c.get("foo", _get_cb)

    def stop(*ar, **kw):
        logging.info('\n\n\nStop server {}_{}'.format(ar, kw))
        # server.stop()
    print("\n\n\nSetting value {0}".format(value))
    c.set("foo", value, 0, _set_cb)

    c.set("bara", {'1': '1'}, 1000, lambda res: stop(server))

    def fut_test():
        print("\n\n\nGet value bara\n\n\n =========================\n")
        c.get('bara', lambda res: stop(res))
        # fut tests
        fut = c.set("future", value, 2)
        print("\n\n\nGet value future bara\n\n\n =========================\n")
        fut_get = c.get('bara')
        fut_del = c.delete('bara')
        server.add_future(
            fut,
            lambda futur: logging.info(
                '\n\n\n\n\n!!!!!!{}!!!!1\n\n\n'
                .format(futur.result())
            )
        )
        server.add_future(
            fut_get,
            lambda futur: logging.info(
                '\n\n\n\n\n222!!!{}!!!!1\n\n\n'
                .format(futur.result())
            )
        )
        server.add_future(
            fut_del,
            lambda futur: logging.info(
                '\n\n\n\n\n333!!!{}!!!!1\n\n\n'
                .format(futur.result())
            )
        )
    server.add_timeout(time.time() + 2, fut_test)
    server.start()
