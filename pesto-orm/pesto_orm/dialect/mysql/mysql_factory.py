# -*- coding:utf8 -*-
import sys
import threading
from enum import Enum

import mysql.connector
import mysql.connector.cursor
import mysql.connector.pooling
from mysql.connector.conversion import MySQLConverter

from pesto_common.config.configer import Configer
from pesto_common.log.logger_factory import LoggerFactory
from pesto_orm.error import reraise, DBError

logger = LoggerFactory.get_logger('dialect.mysql.factory')


class ExecuteMode(Enum):
    ONE_MODE = 0
    MANY_MODE = 1


class ResultMode(Enum):
    STORE_RESULT_MODE = 0
    USE_RESULT_MODE = 1


class CursorMode(Enum):
    CURSOR_MODE = 0
    DICT_CURSOR_MODE = 1


class NumpyMySQLConverter(MySQLConverter):
    """ A mysql.connector Converter that handles Numpy types """

    def _float32_to_mysql(self, value):
        return float(value)

    def _float64_to_mysql(self, value):
        return float(value)

    def _int32_to_mysql(self, value):
        return int(value)

    def _int64_to_mysql(self, value):
        return int(value)


class MysqlExecutor(object):
    """
    Mysql sql 执行工具
    """

    def __init__(self, mysql_connection_pool, show_sql=False):
        self.__mysql_connection_pool = mysql_connection_pool
        self.__show_sql = show_sql

        # 保留本地connection
        self.__local_connection = threading.local()
        self.__local_connection.mysql_connection = None
        self.__local_connection.use_transaction = False

    def close(self):
        self.__close_connection()
        self.__local_connection = None
        self.__mysql_connection_pool = None

    def set_database(self, database):
        self.__mysql_connection_pool.set_config(**{"database": database})

    def __has_connection(self):
        return hasattr(self.__local_connection, "mysql_connection") and self.__local_connection.mysql_connection is not None

    def __has_transaction(self):
        return hasattr(self.__local_connection, "use_transaction") and self.__local_connection.use_transaction

    def __get_connection(self):
        """
        获取一个连接
        """
        if self.__has_connection():
            connection = self.__local_connection.mysql_connection
        else:
            connection = self.__mysql_connection_pool.get_connection()
            self.__local_connection.mysql_connection = connection

        if not connection.is_connected():
            raise DBError(key=DBErrorType.NOT_CONNECT_ERROR, message="Connection is not connect.")

        if self.__has_transaction():
            connection.autocommit = False
        else:
            connection.autocommit = True

        logger.debug('Connection autocommit: {}'.format(connection.autocommit))
        return connection

    def __commit_connection(self):
        """
        关闭当前连接
        """
        if self.__has_connection() and not self.__has_transaction():
            self.__local_connection.mysql_connection.commit()

    def __close_connection(self):
        """
        关闭当前连接
        """
        if self.__has_connection() and not self.__has_transaction():
            self.__local_connection.mysql_connection.close()
            self.__local_connection.mysql_connection = None
            self.__local_connection.use_transaction = False

    def begin_transaction(self):
        self.__local_connection.use_transaction = True

    def commit_transaction(self):
        if self.__has_connection():
            self.__local_connection.mysql_connection.commit()

    def rollback_transaction(self):
        if self.__has_connection():
            self.__local_connection.mysql_connection.rollback()

    def close_transaction(self):
        if self.__has_connection():
            self.__local_connection.mysql_connection.close()
            self.__local_connection.mysql_connection = None
            self.__local_connection.use_transaction = False

    def __execute(self, sql, params=None, execute_mode=ExecuteMode.ONE_MODE, cursor_mode=CursorMode.CURSOR_MODE):
        """
        通用sql执行工具
        """
        if self.__show_sql:
            if isinstance(params, list):
                log_params = params[:5]
                logger.info("Execute Sql: {}, params(top5): \n{}".format(sql, ', \n'.join([str(param_tuple) for param_tuple in log_params])))
            else:
                logger.info("Execute Sql: {}, params: {}".format(sql, params))

        conn = self.__get_connection()
        if cursor_mode == CursorMode.CURSOR_MODE:
            cursor_class = mysql.connector.cursor.MySQLCursor
        elif cursor_mode == CursorMode.DICT_CURSOR_MODE:
            cursor_class = mysql.connector.cursor.MySQLCursorDict
        else:
            raise DBError(key=DBErrorType.CURSOR_MODE_ERROR, message="Cursor mode value is wrong.")

        cursor = conn.cursor(buffered=True, raw=None, prepared=True, cursor_class=cursor_class)

        if execute_mode == ExecuteMode.ONE_MODE:
            cursor.execute(sql, params)
        else:
            cursor.executemany(sql, params)

        return {"conn": conn, "cursor": cursor}

    def execute(self, sql, params=None, execute_mode=ExecuteMode.ONE_MODE, cursor_mode=CursorMode.CURSOR_MODE):
        if sql.upper().startswith('INSERT') | sql.upper().startswith('SELECT') | sql.upper().startswith('UPDATE') | sql.upper().startswith('DELETE'):
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message="Not support this operate")

        conn = None
        cursor = None
        try:
            execute_result = self.__execute(sql=sql, params=params, execute_mode=execute_mode, cursor_mode=cursor_mode)
            conn = execute_result["conn"]
            cursor = execute_result["cursor"]

            self.__commit_connection()
            return True
        except DBError as e:
            raise e
        except Exception as e:
            reraise(DBError(e, sql=sql, params=params), sys.exc_info()[2])
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.__close_connection()

    def insert(self, sql, params=None, execute_mode=ExecuteMode.ONE_MODE, cursor_mode=CursorMode.CURSOR_MODE):

        if sql.upper().startswith('DROP') | sql.upper().startswith('CREATE') | sql.upper().startswith('SELECT') | sql.upper().startswith('UPDATE') | sql.upper().startswith(
                'DELETE'):
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message="Not support this operate")

        conn = None
        cursor = None
        try:
            insert_result = self.__execute(sql=sql, params=params, execute_mode=execute_mode, cursor_mode=cursor_mode)
            conn = insert_result["conn"]
            cursor = insert_result["cursor"]
            rowcount = cursor.rowcount

            if execute_mode == ExecuteMode.ONE_MODE:
                result = cursor.lastrowid
            else:
                result = rowcount

            self.__commit_connection()
            return result
        except DBError as e:
            raise e
        except Exception as e:
            reraise(DBError(e, sql=sql, params=params), sys.exc_info()[2])
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.__close_connection()

    def select_first(self, sql, params=None, cursor_mode=CursorMode.CURSOR_MODE):

        if sql.upper().startswith('DROP') | sql.upper().startswith('CREATE') | sql.upper().startswith('INSERT') | sql.upper().startswith('UPDATE') | sql.upper().startswith(
                'DELETE'):
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message="Not support this operate")

        conn = None
        cursor = None
        try:
            select_result = self.__execute(sql=sql, params=params, cursor_mode=cursor_mode)
            conn = select_result["conn"]
            cursor = select_result["cursor"]

            result = {}
            column_values = cursor.fetchone()
            if column_values is not None and len(column_values) > 0:
                column_names = [i[0] for i in cursor.description]
                for i, value in enumerate(column_values):
                    result[column_names[i]] = value

            return result
        except DBError as e:
            raise e
        except Exception as e:
            reraise(DBError(e, sql=sql, params=params), sys.exc_info()[2])
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.__close_connection()

    def select(self, sql, params=None, cursor_mode=CursorMode.CURSOR_MODE):

        if sql.upper().startswith('DROP') | sql.upper().startswith('CREATE') | sql.upper().startswith('INSERT') | sql.upper().startswith('UPDATE') | sql.upper().startswith(
                'DELETE'):
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message="Not support this operate")

        conn = None
        cursor = None
        try:
            select_result = self.__execute(sql=sql, params=params, cursor_mode=cursor_mode)
            conn = select_result["conn"]
            cursor = select_result["cursor"]

            result = []
            column_values = cursor.fetchall()
            if column_values is not None and len(column_values) > 0:
                column_names = [i[0] for i in cursor.description]

                for i, row in enumerate(column_values):
                    tmp = {}
                    for j, value in enumerate(row):
                        tmp[column_names[j]] = value
                    result.append(tmp)
            return result

        except DBError as e:
            raise e
        except Exception as e:
            reraise(DBError(e, sql=sql, params=params), sys.exc_info()[2])
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.__close_connection()

    def update(self, sql, params=None, cursor_mode=CursorMode.CURSOR_MODE):

        if sql.upper().startswith('DROP') | sql.upper().startswith('CREATE') | sql.upper().startswith('INSERT') | sql.upper().startswith('SELECT') | sql.upper().startswith(
                'DELETE'):
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message="Not support this operate")

        conn = None
        cursor = None
        try:
            update_result = self.__execute(sql=sql, params=params, cursor_mode=cursor_mode)
            conn = update_result["conn"]
            cursor = update_result["cursor"]

            self.__commit_connection()
            return cursor.rowcount
        except DBError as e:
            raise e
        except Exception as e:
            reraise(DBError(e, sql=sql, params=params), sys.exc_info()[2])
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.__close_connection()

    def delete(self, sql, params=None, cursor_mode=CursorMode.CURSOR_MODE):

        if sql.upper().startswith('DROP') | sql.upper().startswith('CREATE') | sql.upper().startswith('INSERT') | sql.upper().startswith('SELECT') | sql.upper().startswith(
                'UPDATE'):
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message="Not support this operate")

        conn = None
        cursor = None
        try:
            delete_result = self.__execute(sql=sql, params=params, cursor_mode=cursor_mode)
            conn = delete_result["conn"]
            cursor = delete_result["cursor"]

            self.__commit_connection()
            return cursor.rowcount
        except DBError as e:
            raise e
        except Exception as e:
            reraise(DBError(e, sql=sql, params=params), sys.exc_info()[2])
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.__close_connection()


class DBErrorType(Enum):
    NOT_CONNECT_ERROR = 100001
    CURSOR_MODE_ERROR = 100002
    OPERATE_NOT_SUPPORT_ERROR = 100003
    SQL_BUILD_ERROR = 100004


class MysqlFactory(object):
    """
    Mysql 连接池管理器
    """
    __db_connection_pools = {str: MysqlExecutor}

    def __init__(self):
        pass

    @staticmethod
    def __get_pool_key(db_config):
        if 'database' not in db_config:
            key = db_config['host'] + ":" + str(db_config['port']) + ":" + 'None'
        else:
            key = db_config['host'] + ":" + str(db_config['port']) + ":" + db_config['database']
        return key

    @staticmethod
    def get_executor(db_config, pool_name="mysql-pool", pool_size=32, pool_reset_session=True):
        key = MysqlFactory.__get_pool_key(db_config)

        try:
            return MysqlFactory.__db_connection_pools[key]
        except KeyError:
            try:
                _db_config = db_config.copy()
                if 'password' in _db_config:
                    _db_config['password'] = '******'
                logger.info(
                    "Init mysql pool info - name: " + pool_name + ", size: " + str(pool_size) + ", reset session: " + str(pool_reset_session) + ", dbconfig: " + str(_db_config))
                __mysql_connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name=pool_name, pool_size=pool_size, pool_reset_session=pool_reset_session, **db_config)
                MysqlFactory.__db_connection_pools[key] = MysqlExecutor(__mysql_connection_pool, True if Configer.get("db.show_sql") == "True" else False)
            except Exception as e:
                raise DBError(e)
            return MysqlFactory.__db_connection_pools[key]

    @staticmethod
    def remove_executor(db_config):
        key = MysqlFactory.__get_pool_key(db_config)
        try:
            MysqlFactory.__db_connection_pools[key].close()
            del MysqlFactory.__db_connection_pools[key]
        except KeyError:
            pass
        return True
