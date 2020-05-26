from pesto_orm.dialect.postgre_sql.domain import postgreSQLDialect

from pesto_orm.dialect.mysql.domain import mysqlDialect


# 数据库方言列表
class DialectFactory(object):
    dialects = {
        postgreSQLDialect.get_db_type(): postgreSQLDialect,
        mysqlDialect.get_db_type(): mysqlDialect
    }

    @staticmethod
    def get_dialect(type):
        return DialectFactory.dialects[type]
