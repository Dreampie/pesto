# coding=utf-8
import traceback

from pesto_common.config.configer import Configer
from pesto_common.log.logger_factory import LoggerFactory
from pesto_orm.dialect.mysql.mysql_factory import MysqlFactory
from pesto_orm.dialect.mysql.mysql_factory import NumpyMySQLConverter

logger = LoggerFactory.get_logger('dialect.mysql.domain')

mysqlExecutor = None
if Configer.contains('db.'):
    db_config = {
        'user': Configer.get("db.user"),
        'password': Configer.get("db.password"),
        'host': Configer.get("db.host"),
        'port': Configer.get("db.port"),
        'database': Configer.get("db.database"),
        'raise_on_warnings': bool(Configer.get("db.raise_on_warnings", False)),
        'charset': Configer.get("db.charset", 'utf8mb4'),
        'connection_timeout': int(Configer.get('db.connection_timeout', 180)),
        'autocommit': bool(Configer.get('db.autocommit', True)),
        'converter_class': NumpyMySQLConverter
    }
    mysqlExecutor = MysqlFactory.get_executor(db_config)


def transaction(rollback_exceptions=[]):
    def wrap(func):
        def handle(result, **kwargs):  # 真实执行原方法.
            func = kwargs['func']
            args = kwargs['args']
            kwargs = kwargs['kwargs']
            return_value = func(*args, **kwargs)
            logger.info("Transaction method: " + func.__name__)
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
                    logger.error("Method execute error. method: " + str(func.__name__) + ",  error:" + traceback.format_exc() + ", transaction roll back.")
                else:
                    mysqlExecutor.commit_transaction()
                    raise e
            finally:
                mysqlExecutor.close_transaction()

        return to_do

    return wrap
