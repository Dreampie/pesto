import sys

from pesto_orm.core.error import DBError


class InvalidConnection(DBError):
    pass


class Connection(object):
    def __init__(self, target, *args, **kwargs):
        self._conn = None
        self._closed = True
        self._transaction = False
        self._usage = 0
        try:
            self._target = target.connect
            self._db_api = target
        except AttributeError:
            self._target = target
            try:
                self._db_api = target.dbapi
            except AttributeError:
                try:
                    self._db_api = sys.modules[target.__module__]
                    if self._db_api.connect != target:
                        raise AttributeError
                except (AttributeError, KeyError):
                    self._db_api = None
        if not callable(self._target):
            raise TypeError("%r is not a connection provider." % (target,))

        self._args, self._kwargs = args, kwargs
        self.__connect()

    def __connect(self):
        try:
            conn = self._target(*self._args, **self._kwargs)
            if self._db_api is None or self._db_api.connect != self._target:
                module = conn.__module__ if hasattr(conn, '__module__') else None
                while module:
                    try:
                        self._db_api = sys.modules[module]
                        if callable(self._db_api.connect):
                            break
                    except (AttributeError, KeyError):
                        pass
                    i = module.rfind('.')
                    if i < 0:
                        module = None
                    else:
                        module = module[:i]
                else:
                    self._db_api = None
            # close pre conn
            if not self._closed:
                self.close()
            self._conn = conn
            self._transaction = False
            self._closed = False
            self._usage = 0
        except Exception as e:
            self.close()
            raise e

    def is_connected(self):
        try:
            return self._conn.is_connected()
        except Exception as e:
            pass
        return True

    def cursor(self, *args, **kwargs):
        return Cursor(self._conn, self._conn.cursor(*args, **kwargs))

    def begin(self, *args, **kwargs):
        self._transaction = True
        try:
            self._conn.begin(*args, **kwargs)
        except AttributeError:
            pass

    def commit(self):
        self._transaction = False
        try:
            self._conn.commit()
        except Exception as e:  # cannot commit
            try:  # try to reopen the connection
                self.__connect()
            except Exception:
                pass
            raise e

    def rollback(self):
        self._transaction = False
        try:
            self._conn.rollback()
        except Exception as e:  # cannot rollback
            try:  # try to reopen the connection
                self.__connect()
            except Exception:
                pass
            else:
                self.close()
            raise e

    def close(self):
        if not self._closed:
            try:
                if self._transaction:
                    self.reset()
                else:
                    self._conn.close()
            except Exception as e:
                pass
            self._transaction = False
            self._closed = True

    def cancel(self):
        self._transaction = False
        try:
            self._conn.cancel()
        except Exception as e:
            pass

    def reset(self):
        if not self._closed and self._transaction:
            try:
                self.rollback()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if exc[0] is None and exc[1] is None and exc[2] is None:
            self.commit()
        else:
            self.rollback()

    def __del__(self):
        self.close()


class InvalidCursor(DBError):
    pass


class Cursor(object):
    def __init__(self, conn, cursor):
        self._closed = True
        self._conn = conn
        self._cursor = cursor
        self._closed = False

    def close(self):
        if not self._closed:
            try:
                self._cursor.close()
            except Exception as e:
                pass
            self._closed = True

    def __getattr__(self, name):
        if self._cursor:
            return getattr(self._cursor, name)
        else:
            raise InvalidCursor

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def __del__(self):
        self.close()
