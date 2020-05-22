# -*- coding:utf8 -*-
from abc import ABCMeta, abstractmethod

from pesto_common.utils.reflect_utils import ReflectUtils
from pesto_orm.error import BaseError


class BaseRepository(object):
    __metaclass__ = ABCMeta

    def __init__(self, model_class, module=None, class_name=None):
        self.model_class = model_class
        self.module = module  # model_class.__module__
        self.class_name = class_name  # model_class.__name__

    def _create_instance(self, *args, **kwargs):
        if self.model_class is not None:
            return self.model_class(*args, **kwargs)
        elif self.module is not None and self.class_name is not None:
            obj = ReflectUtils.create_instance(module=self.module, class_name=self.class_name, *args, **kwargs)
            return obj
        else:
            return {}

    def _assembly_columns(self, columns):
        if len(columns) == 1 and columns[0] == "*":
            select_sql = "*"
        else:
            select_columns = []
            if isinstance(columns, list):
                for column in columns:
                    if column.startswith('`'):
                        select_columns.append(column)
                    else:
                        select_columns.append("`" + column + "`")
            elif isinstance(columns, dict):
                for column in columns:
                    select_columns.append("`{}` as `{}`".format(column, columns[column]))
            else:
                raise BaseError(message='columns type not support.')
            separator = ", "
            select_sql = separator.join(select_columns)
        return select_sql

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
    def build_query_sql(self, columns=["*"], where="", params=None):
        raise NotImplementedError

    """
    :return list of model
    """

    def query_by(self, columns=["*"], where="", params=None, yield_able=False):
        sql, params = self.build_query_sql(columns=columns, where=where, params=params)
        return self.query(sql=sql, params=params, yield_able=yield_able)

    @abstractmethod
    def query(self, sql='', params=None, yield_able=False):
        raise NotImplementedError

    @abstractmethod
    def build_query_first_sql(self, columns=["*"], where="", params=None):
        raise NotImplementedError

    def query_first_by(self, columns=["*"], where="", params=None):
        sql, params = self.build_query_first_sql(columns=columns, where=where, params=params)
        return self.query_first(sql=sql, params=params)

    @abstractmethod
    def query_first(self, sql='', params=None):
        raise NotImplementedError

    @abstractmethod
    def build_page_sql(self, columns=["*"], where="", page_num=1, page_size=1, params=None):
        raise NotImplementedError

    def page_by(self, columns=["*"], where="", page_num=1, page_size=1, params=None, yield_able=False):
        sql, params = self.build_page_sql(columns=columns, where=where, page_num=page_num, page_size=page_size, params=params)
        return self.page(sql=sql, params=params, yield_able=yield_able)

    @abstractmethod
    def page(self, sql='', params=None, yield_able=False):
        raise NotImplementedError

    """
    构建更新语句, sql,params
    """

    @abstractmethod
    def build_update_sql(self, models=[], columns=[], where="", params=None):
        raise NotImplementedError

    """
    :return update row length
    """

    def update_by(self, models=[], columns=[], where="", params=None):
        sql, params = self.build_update_sql(models=models, columns=columns, where=where, params=params)
        return self.update(sql=sql, params=params)

    @abstractmethod
    def update(self, sql='', params=None):
        raise NotImplementedError

    """
    构建删除语句: sql, params
    """

    @abstractmethod
    def build_delete_sql(self, models=[], where="", params=None):
        raise NotImplementedError

    """
    :return delete row length
    """

    def delete_by(self, models=[], where="", params=None):
        sql, params = self.build_delete_sql(models=models, where=where, params=params)
        return self.delete(sql=sql, params=params)

    @abstractmethod
    def delete(self, sql='', params=None):
        raise NotImplementedError

    """
    build insert sql: 
    """

    @abstractmethod
    def build_insert_sql(self, models=[]):
        raise NotImplementedError

    """
    :return insert models
    """

    def insert_by(self, models=[]):
        sql, params = self.build_insert_sql(models=models)
        return self.insert(sql=sql, params=params)

    """
    insert rows
    """

    @abstractmethod
    def insert(self, sql='', params=[()]):
        raise NotImplementedError
