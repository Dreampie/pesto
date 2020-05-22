# -*- coding:utf8 -*-

from pesto_common.log.logger_factory import LoggerFactory
from pesto_orm.dialect.mysql.domain import mysqlExecutor
from pesto_orm.dialect.mysql.mysql_factory import ExecuteMode, DBErrorType
from pesto_orm.error import DBError
from pesto_orm.repository import BaseRepository

logger = LoggerFactory.get_logger('dialect.mysql.repository')


class MysqlBaseRepository(BaseRepository):

    def __init__(self, model_class=None):
        super(MysqlBaseRepository, self).__init__(model_class)
        self.db_name = None
        self.table_name = None
        self.primary_key = None
        self.from_sql = None

        model = self._create_instance()
        if not isinstance(model, dict):
            if hasattr(model, "db.name"):
                self.db_name = model.db_name
            if hasattr(model, "table_name"):
                self.table_name = model.table_name
            if hasattr(model, "primary_key"):
                self.primary_key = model.primary_key

    def _assembly_from_sql(self):
        if self.from_sql is None:
            self.from_sql = ("`" + self.db_name + "`." if self.db_name is not None else "") + ("`" + self.table_name + "`" if self.table_name is not None else "")
        return self.from_sql

    def build_query_sql(self, columns=["*"], where="", params=None):
        sql = "SELECT " + self._assembly_columns(columns) + " FROM " + self._assembly_from_sql() + (" WHERE " + where if where != "" else "")
        return sql, params

    def query(self, sql='', params=None, yield_able=False):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        query_result = mysqlExecutor.select(sql=sql, params=params)

        if yield_able:
            return self._yield_result(query_result)
        else:
            return self._return_result(query_result)

    def build_query_first_sql(self, columns=["*"], where="", params=None):

        sql = "SELECT " + self._assembly_columns(columns) + " FROM " + self._assembly_from_sql() + (" WHERE " + where if where != "" else "") + " LIMIT 1"
        return sql, params

    def query_first(self, sql='', params=None):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        query_result = mysqlExecutor.select_first(sql=sql, params=params)

        result = None
        if query_result is not None and len(query_result) > 0:
            result = self._create_instance()
            if isinstance(result, dict):
                result.update(query_result.copy())
            else:
                result.set_attrs(query_result.copy())
        return result

    def build_page_sql(self, columns=["*"], where="", page_num=1, page_size=1, params=None):
        sql = "SELECT " + self._assembly_columns(columns) + " FROM " + self._assembly_from_sql() + (" WHERE " + where if where != "" else ""
                                                                                                    ) + " LIMIT " + str(page_size * (page_num - 1)) + "," + str(page_size)
        return sql, params

    def page(self, sql='', params=None, yield_able=False):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))
        return self.query(sql=sql, params=params, yield_able=yield_able)

    """
    构建更新语句
    """

    def build_update_sql(self, models=[], columns=[], where="", params=None):
        if len(models) > 0:
            # 批量更新
            sql = "";
            columns = models[0].get_attrs().keys()
            if len(columns) <= 0:
                raise DBError(key=DBErrorType.SQL_BUILD_ERROR, message="Not found any columns to update.")
            else:
                for model in models:
                    params = []
                    set_columns = []
                    for column in columns:
                        if column != self.primary_key:
                            set_columns.append("`" + column + "`= %s")
                            params.append(model.get_attr(column))
                    separator = ", "
                    set_sql = separator.join(set_columns)

                    primary_value = model.get_attr(self.primary_key)
                    where = "`%s` = %s" % (self.primary_key, primary_value)
                    params.append(primary_value)

                    sql += "UPDATE " + self._assembly_from_sql() + " SET " + set_sql + (" WHERE " + where if where != "" else "") + ";"
        else:
            if len(columns) <= 0:
                raise DBError(key=DBErrorType.SQL_BUILD_ERROR, message="Not found any columns to update.")
            else:
                set_columns = []
                for column in columns:
                    if column != self.primary_key:
                        set_columns.append("`" + column + "`= %s")

                separator = ", "
                set_sql = separator.join(set_columns)

            sql = "UPDATE " + self._assembly_from_sql() + " SET " + set_sql + (" WHERE " + where if where != "" else "")

        return sql, params

    def update(self, sql='', params=None):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        return mysqlExecutor.update(sql=sql, params=params)

    """
    构建删除语句
    """

    def build_delete_sql(self, models=[], where="", params=None):
        if len(models) > 0:
            separator = ", "

            params = []
            for model in models:
                params.append(model.get_attr(self.primary_key))
            value_holders = ['%s'] * len(params)
            where = '`%s` in (%s)' % (self.primary_key, separator.join(value_holders))

        sql = "DELETE FROM " + self._assembly_from_sql() + (" WHERE " + where if where != "" else "")
        return sql, params

    def delete(self, sql='', params=None):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        return mysqlExecutor.delete(sql, params)

    """
    build insert sql
    """

    def build_insert_sql(self, models=[]):

        separator = ", "
        params = []
        first_model = models[0]
        columns = first_model.get_attrs().keys()
        value_holders = ['%s'] * len(columns)

        for model in models:
            values = []
            attrs = model.get_attrs()
            for column in columns:
                values.append(attrs[column])

            params.append(tuple(values))

        sql = "INSERT INTO " + self._assembly_from_sql() + "(" + separator.join(columns) + ") VALUES (" + separator.join(
            value_holders) + ")"
        return sql, params

    """
    :return insert rows
    """

    def insert_by(self, models=[]):
        sql, params = self.build_insert_sql(models=models)

        return self.insert(sql=sql, params=params)

    """
    insert list
    """

    def insert(self, sql='', params=[()]):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        return mysqlExecutor.insert(sql=sql, params=params, execute_mode=ExecuteMode.MANY_MODE)
