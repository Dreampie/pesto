from threading import Condition

from pesto_common.log.logger_factory import LoggerFactory
from pesto_orm.core.error import DBError
from pesto_orm.db.connection import Connection, InvalidConnection

logger = LoggerFactory.get_logger('db.pool')


class TooManyConnections(DBError):
    pass


class ConnectionPool(object):
    def __init__(self, target, core_size=20, max_size=100, max_wait=100, *args, **kwargs):
        self._target = target
        self._core_size = core_size
        self._max_size = max_size
        self._max_wait = max_wait
        self._curr_size = 0
        self._curr_wait = 0
        self._lock = Condition()
        self._args, self._kwargs = args, kwargs
        self._using_conns = []
        self._idle_conns = [self.__connection() for i in range(core_size)]

    def __connection(self):
        while self._curr_size > self._max_size:
            if self._curr_wait > self._max_wait:
                raise TooManyConnections
            self._curr_wait += 1
            self._lock.wait()
        self._curr_wait -= 1
        conn = Connection(target=self._target, *self._args, **self._kwargs)
        self._curr_size += 1
        return PoolConnection(pool=self, conn=conn)

    def get_connection(self):
        self._lock.acquire()
        try:
            if len(self._idle_conns) > 0:
                conn = self._idle_conns.pop(0)
            else:
                conn = self.__connection()

            self._using_conns.append(conn)
            self._lock.notify()
        finally:
            self._lock.release()
        return conn

    def return_connection(self, conn):
        self._lock.acquire()
        try:
            if len(self._idle_conns) < self._core_size:
                conn.reset()
                self._idle_conns.append(conn)
            else:
                conn.close()

            self._lock.notify()
        finally:
            self._lock.release()

    def close(self):
        self._lock.acquire()
        try:
            while self._idle_conns:  # close all idle connections
                conn = self._idle_conns.pop(0)
                try:
                    conn.close()
                    self._curr_size -= 1
                except Exception:
                    pass
            while self._using_conns:
                conn = self._using_conns.pop(0)
                try:
                    conn.close()
                    self._curr_size -= 1
                except Exception:
                    pass

            self._lock.notifyAll()
        finally:
            self._lock.release()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class PoolConnection(object):
    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def close(self):
        if self._conn:
            self._pool.return_connection(self._conn)
            self._conn = None

    def __getattr__(self, name):
        if self._conn:
            return getattr(self._conn, name)
        else:
            raise InvalidConnection

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
