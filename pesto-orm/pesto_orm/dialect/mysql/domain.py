# coding=utf-8
import re
import traceback

from pesto_common.config.configer import Configer
from pesto_common.log.logger_factory import LoggerFactory
from pesto_orm.core.base import db_config
from pesto_orm.core.executor import ExecutorFactory
from pesto_orm.core.model import BaseModel
from pesto_orm.core.repository import BaseRepository
from pesto_orm.dialect.base import DefaultDialect

logger = LoggerFactory.get_logger('dialect.mysql.domain')


class MySQLDialect(DefaultDialect):

    def get_db_type(self):
        return 'mysql'

    def paginate_with(self, sql, page_number, page_size):
        if page_number == 1 and page_size == 1:
            if re.match(DefaultDialect.select_single_pattern, sql) is not None:
                return sql

        offset = page_size * (page_number - 1)
        return '%s LIMIT %d OFFSET %d' % (sql, page_size, offset)


db_type = Configer.get('db.type')
if db_type == 'mysql':
    import mysql.connector as connector

    db_config['target'] = connector
    db_config['use_pure'] = True

    from mysql.connector.conversion import MySQLConverter


    class NumpyMySQLConverter(MySQLConverter):
        ''' A mysql.connector Converter that handles Numpy types '''

        def _float32_to_mysql(self, value):
            return float(value)

        def _float64_to_mysql(self, value):
            return float(value)

        def _int32_to_mysql(self, value):
            return int(value)

        def _int64_to_mysql(self, value):
            return int(value)


    db_config['converter_class'] = NumpyMySQLConverter

    mysqlExecutor = ExecutorFactory.get_executor(db_config=db_config)

    mysqlDialect = MySQLDialect()


class MysqlBaseModel(BaseModel):

    def __init__(self, db_name=None, table_name=None, table_alias=None, primary_key='id'):
        super(MysqlBaseModel, self).__init__(db_name, table_name, table_alias, primary_key)

    def get_dialect(self):
        return mysqlDialect

    def get_executor(self):
        return mysqlExecutor


class MysqlBaseRepository(BaseRepository):

    def __init__(self, model_class=None):
        super(MysqlBaseRepository, self).__init__(model_class)

    def get_dialect(self):
        return mysqlDialect

    def get_executor(self):
        return mysqlExecutor


def transaction(rollback_exceptions=[]):
    def wrap(func):
        def handle(result, **kwargs):  # 真实执行原方法.
            func = kwargs['func']
            args = kwargs['args']
            kwargs = kwargs['kwargs']
            return_value = func(*args, **kwargs)
            logger.info('Transaction method: ' + func.__name__)
            result.append(return_value)

        def to_do(*args, **kwargs):
            new_kwargs = {'func': func, 'args': args, 'kwargs': kwargs}

            result = []
            global mysqlExecutor
            try:
                mysqlExecutor.begin_transaction()
                handle(result, **new_kwargs)
                mysqlExecutor.commit_transaction()
            except Exception as e:

                if len(rollback_exceptions) == 0 or e.__class__ in rollback_exceptions:
                    mysqlExecutor.rollback_transaction()
                    logger.error('Method execute error. method: ' + str(func.__name__) + ',  error:' + traceback.format_exc() + ', transaction roll back.')
                else:
                    mysqlExecutor.commit_transaction()
                    raise e
            finally:
                mysqlExecutor.close_transaction()

        return to_do

    return wrap
