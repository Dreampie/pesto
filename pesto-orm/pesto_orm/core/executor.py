import sys
import threading
from abc import ABCMeta

from pesto_common.log.logger_factory import LoggerFactory
from pesto_orm.core.base import ExecuteMode, CursorMode
from pesto_orm.core.error import DBErrorType, DBError, reraise
from pesto_orm.db.pool import ConnectionPool

logger = LoggerFactory.get_logger('core.executor')


class Executor(object):
    __metaclass__ = ABCMeta

    def __init__(self, pool, show_sql=False):
        self.__pool = pool
        self.__show_sql = show_sql

        # 保留本地conn
        self.__local_conn = threading.local()
        self.__local_conn.conn = None
        self.__local_conn.use_transaction = False

    def show_sql(self, sql, params=None):
        if isinstance(params, list) and isinstance(params[0], tuple):
            log_params = params[:5]
            logger.info('Execute sql: {}, params(top5): \n{}'.format(sql, ', \n'.join([str(param_tuple) for param_tuple in log_params])))
        else:
            logger.info('Execute sql: {}, params: {}'.format(sql, params))

    def set_database(self, database):
        self.__pool.set_config(**{'database': database})

    def __has_connection(self):
        return hasattr(self.__local_conn, 'conn') and self.__local_conn.conn is not None

    def __has_transaction(self):
        return hasattr(self.__local_conn, 'use_transaction') and self.__local_conn.use_transaction

    def __get_connection(self):
        '''
        获取一个连接
        '''
        if self.__has_connection():
            conn = self.__local_conn.conn
        else:
            conn = self.__pool.get_connection()
            self.__local_conn.conn = conn

        if not conn.is_connected():
            raise DBError(key=DBErrorType.NOT_CONNECT_ERROR, message='Connection is not connect.')

        if self.__has_transaction():
            conn.autocommit = False
        else:
            conn.autocommit = True

        logger.debug('Connection autocommit: {}'.format(conn.autocommit))
        return conn

    def __commit_connection(self):
        '''
        关闭当前连接
        '''
        if self.__has_connection() and not self.__has_transaction():
            self.__local_conn.conn.commit()

    def __close_connection(self):
        '''
        关闭当前连接
        '''
        if self.__has_connection() and not self.__has_transaction():
            self.__local_conn.conn.close()
            self.__local_conn.conn = None
            self.__local_conn.use_transaction = False

    def begin_transaction(self):
        self.__local_conn.use_transaction = True
        self.__local_conn.conn.begin()

    def commit_transaction(self):
        if self.__has_connection():
            self.__local_conn.conn.commit()

    def rollback_transaction(self):
        if self.__has_connection():
            self.__local_conn.conn.rollback()

    def close_transaction(self):
        if self.__has_connection():
            self.__local_conn.conn.close()
            self.__local_conn.conn = None
            self.__local_conn.use_transaction = False

    def __execute(self, sql, params=None, execute_mode=ExecuteMode.ONE_MODE, cursor_mode=CursorMode.CURSOR_MODE):
        '''
        通用sql执行工具
        '''
        if self.__show_sql:
            self.show_sql(sql=sql, params=params)

        conn = self.__get_connection()
        cursor = conn.cursor()

        if execute_mode == ExecuteMode.ONE_MODE:
            cursor.execute(sql, params)
        else:
            cursor.executemany(sql, params)

        return {'conn': conn, 'cursor': cursor}

    def execute(self, sql, params=None, execute_mode=ExecuteMode.ONE_MODE, cursor_mode=CursorMode.CURSOR_MODE):
        if sql.upper().startswith('INSERT') | sql.upper().startswith('SELECT') | sql.upper().startswith('UPDATE') | sql.upper().startswith('DELETE'):
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message='Not support this operate')

        conn = None
        cursor = None
        try:
            execute_result = self.__execute(sql=sql, params=params, execute_mode=execute_mode, cursor_mode=cursor_mode)
            conn = execute_result['conn']
            cursor = execute_result['cursor']

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
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message='Not support this operate')

        conn = None
        cursor = None
        try:
            insert_result = self.__execute(sql=sql, params=params, execute_mode=execute_mode, cursor_mode=cursor_mode)
            conn = insert_result['conn']
            cursor = insert_result['cursor']
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
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message='Not support this operate')

        conn = None
        cursor = None
        try:
            select_result = self.__execute(sql=sql, params=params, cursor_mode=cursor_mode)
            conn = select_result['conn']
            cursor = select_result['cursor']

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
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message='Not support this operate')

        conn = None
        cursor = None
        try:
            select_result = self.__execute(sql=sql, params=params, cursor_mode=cursor_mode)
            conn = select_result['conn']
            cursor = select_result['cursor']

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
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message='Not support this operate')

        conn = None
        cursor = None
        try:
            update_result = self.__execute(sql=sql, params=params, cursor_mode=cursor_mode)
            conn = update_result['conn']
            cursor = update_result['cursor']

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
            raise DBError(key=DBErrorType.OPERATE_NOT_SUPPORT_ERROR, message='Not support this operate')

        conn = None
        cursor = None
        try:
            delete_result = self.__execute(sql=sql, params=params, cursor_mode=cursor_mode)
            conn = delete_result['conn']
            cursor = delete_result['cursor']

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

    def close(self):
        self.__pool.close()
        self.__local_conn = None
        self.__pool = None


class ExecutorFactory(object):
    '''
    连接池管理器
    '''

    __connection_pools = {}
    __lock = threading.Condition()

    def __init__(self):
        pass

    @staticmethod
    def __get_pool_key(db_config):
        if 'database' not in db_config:
            key = db_config['host'] + ':' + str(db_config['port']) + ':' + 'None'
        else:
            key = db_config['host'] + ':' + str(db_config['port']) + ':' + db_config['database']
        return key

    @staticmethod
    def get_executor(db_config):
        key = ExecutorFactory.__get_pool_key(db_config)

        if key not in ExecutorFactory.__connection_pools:
            ExecutorFactory.__lock.acquire()
            try:
                _db_config = db_config.copy()
                if 'password' in _db_config:
                    _db_config['password'] = '******'
                logger.info(
                    'Init mysql pool info -  db config: %s' % str(_db_config))
                target = db_config['target']
                del db_config['target']
                show_sql = db_config['show_sql']
                del db_config['show_sql']
                pool = ConnectionPool(target=target, **db_config)

                ExecutorFactory.__connection_pools[key] = Executor(pool=pool, show_sql=show_sql)
                ExecutorFactory.__lock.notifyAll()
            except Exception as e:
                reraise(DBError(e), sys.exc_info()[2])
            finally:
                ExecutorFactory.__lock.release()
        return ExecutorFactory.__connection_pools[key]

    @staticmethod
    def remove_executor(db_config):
        key = ExecutorFactory.__get_pool_key(db_config)
        try:
            ExecutorFactory.__connection_pools[key].close()
            del ExecutorFactory.__connection_pools[key]
        except KeyError:
            pass
        return True
