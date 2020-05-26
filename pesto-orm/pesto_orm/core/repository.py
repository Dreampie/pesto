# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod

from pesto_common.log.logger_factory import LoggerFactory
from pesto_common.utils.reflect_utils import ReflectUtils
from pesto_orm.core.base import ExecuteMode
from pesto_orm.core.error import DBError, DBErrorType

logger = LoggerFactory.get_logger('core.repository')


class BaseRepository(object):
    __metaclass__ = ABCMeta

    def __init__(self, model_class, module=None, class_name=None):
        self.model_class = model_class
        self.module = module  # model_class.__module__
        self.class_name = class_name  # model_class.__name__

        self.db_name = None
        self.table_name = None
        self.table_alias = None
        self.primary_key = None
        self.sequence = None

        model = self._create_instance()
        if not isinstance(model, dict):
            if hasattr(model, 'db.name'):
                self.db_name = model.db_name
            if hasattr(model, 'table_name'):
                self.table_name = model.table_name
            if hasattr(model, 'table_alias'):
                self.table_alias = model.table_alias
            if hasattr(model, 'primary_key'):
                self.primary_key = model.primary_key
            if hasattr(model, 'sequence'):
                self.sequence = model.sequence

    def _create_instance(self, *args, **kwargs):
        if self.model_class is not None:
            return self.model_class(*args, **kwargs)
        elif self.module is not None and self.class_name is not None:
            obj = ReflectUtils.create_instance(module=self.module, class_name=self.class_name, *args, **kwargs)
            return obj
        else:
            return {}

    def _yield_result(self, query_result):
        if query_result is not None and len(query_result) > 0:

            for row in query_result:
                model = self._create_instance()
                if isinstance(model, dict):
                    model.update(row.copy())
                else:
                    model.set_attrs(row.copy())
                yield model

    def _return_result(self, query_result):
        result = []
        if query_result is not None and len(query_result) > 0:
            for row in query_result:
                model = self._create_instance()
                if isinstance(model, dict):
                    model.update(row.copy())
                else:
                    model.set_attrs(row.copy())
                result.append(model)
        return result

    @abstractmethod
    def get_dialect(self):
        raise NotImplementedError

    @abstractmethod
    def get_executor(self):
        raise NotImplementedError

    def build_query_sql(self, columns=['*'], where='', params=None):
        sql = self.get_dialect().select(columns=columns, table=self.table_name, alias=self.table_alias, where=where)
        return sql, params

    def query_by(self, columns=['*'], where='', params=None, yield_able=False):
        sql, params = self.build_query_sql(columns=columns, where=where, params=params)
        return self.query(sql=sql, params=params, yield_able=yield_able)

    def query(self, sql='', params=None, yield_able=False):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        query_result = self.get_executor().select(sql=sql, params=params)

        if yield_able:
            return self._yield_result(query_result)
        else:
            return self._return_result(query_result)

    def build_query_first_sql(self, columns=['*'], where='', params=None):
        sql = self.get_dialect().paginate(columns=columns, table=self.table_name, alias=self.table_alias, where=where, page_number=1, page_size=1)
        return sql, params

    def query_first_by(self, columns=['*'], where='', params=None):
        sql, params = self.build_query_first_sql(columns=columns, where=where, params=params)
        return self.query_first(sql=sql, params=params)

    def query_first(self, sql='', params=None):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        query_result = self.get_executor().select_first(sql=sql, params=params)

        result = None
        if query_result is not None and len(query_result) > 0:
            result = self._create_instance()
            if isinstance(result, dict):
                result.update(query_result.copy())
            else:
                result.set_attrs(query_result.copy())
        return result

    def build_page_sql(self, columns=['*'], where='', page_num=1, page_size=1, params=None):
        sql = self.get_dialect().paginate(columns=columns, table=self.table_name, alias=self.table_alias, where=where, page_number=page_num, page_size=page_size)
        return sql, params

    def page_by(self, columns=['*'], where='', page_num=1, page_size=1, params=None, yield_able=False):
        sql, params = self.build_page_sql(columns=columns, where=where, page_num=page_num, page_size=page_size, params=params)
        return self.page(sql=sql, params=params, yield_able=yield_able)

    def page(self, sql='', params=None, yield_able=False):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))
        return self.query(sql=sql, params=params, yield_able=yield_able)

    def build_update_sql(self, models=[], columns=[], where='', params=None):
        if len(models) > 0:
            # 批量更新
            sql = ''
            columns = models[0].get_attrs().keys()
            if len(columns) <= 0:
                raise DBError(key=DBErrorType.SQL_BUILD_ERROR, message='Not found any columns to update.')
            else:
                for model in models:
                    params = []
                    for column in columns:
                        if column != self.primary_key:
                            params.append(model.get_attr(column))

                    primary_value = model.get_attr(self.primary_key)
                    params.append(primary_value)

                    sql += self.get_dialect().update(columns=columns, table=self.table_name, alias=self.table_alias, where='`%s` = %s;' % (self.primary_key, primary_value))

        else:
            sql = self.get_dialect().update(columns=columns, table=self.table_name, alias=self.table_alias, where=where)

        return sql, params

    def update_by(self, models=[], columns=[], where='', params=None):
        sql, params = self.build_update_sql(models=models, columns=columns, where=where, params=params)
        return self.update(sql=sql, params=params)

    def update(self, sql='', params=None):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        return self.get_executor().update(sql=sql, params=params)

    def build_delete_sql(self, models=[], where='', params=None):
        if len(models) > 0:
            params = []
            for model in models:
                params.append(model.get_attr(self.primary_key))

            where = '`%s` in (%s)' % (self.primary_key, self.get_dialect().get_placeholders(len(models)))

        sql = self.get_dialect().delete(table=self.table_name, where=where)
        return sql, params

    def delete_by(self, models=[], where='', params=None):
        sql, params = self.build_delete_sql(models=models, where=where, params=params)
        return self.delete(sql=sql, params=params)

    def delete(self, sql='', params=None):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        return self.get_executor().delete(sql=sql, params=params)

    def build_insert_sql(self, models=[]):
        params = []
        first_model = models[0]
        columns = first_model.get_attrs().keys()

        for model in models:
            values = []
            attrs = model.get_attrs()
            for column in columns:
                values.append(attrs[column])

            params.append(tuple(values))

        sql = self.get_dialect().insert(columns=columns, table=self.table_name, primary_key=self.primary_key, sequence=self.sequence)
        return sql, params

    def insert_by(self, models=[]):
        sql, params = self.build_insert_sql(models=models)
        return self.insert(sql=sql, params=params)

    def insert(self, sql='', params=[()]):
        if self.table_name is not None and self.table_name not in sql:
            logger.warning('This model only for table {}, please check sql, is it contains this table?'.format(self.table_name))

        return self.get_executor().insert(sql=sql, params=params, execute_mode=ExecuteMode.MANY_MODE)
