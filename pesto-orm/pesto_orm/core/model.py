'''
orm 基础对象结构
'''
import datetime
import json
from abc import abstractmethod, ABCMeta

from json import JSONEncoder


def _default(self, obj):
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    elif isinstance(obj, BaseModel):
        return obj.get_attrs()
    return _default.default(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class Model(object):
    __metaclass__ = ABCMeta

    def __init__(self, exclude_attrs=[], renames={}, init_attrs={}):
        '''
        init  model
        :param exclude_attrs: 不作为对象属性的值，比如：orm中的table_name 等一些meta信息
        :param renames: 属性重命名，原本 a=5，可以重命名为 b=5, 用于纠正一些原生对象的不规范属性名称
        '''
        exclude_attrs.append('_attrs')
        exclude_attrs.append('_renames')

        news_exclude_attrs = list(set(exclude_attrs))
        news_exclude_attrs.sort(key=exclude_attrs.index)
        self._exclude_attrs = news_exclude_attrs
        self._attrs = {}
        self._renames = renames

        self.set_attrs(init_attrs)

    def __getattr__(self, key):
        if key.startswith('__') and key.endswith('__'):
            return super(Model, self).__getattribute__(key)

        if key == '_exclude_attrs' or key in self.__dict__['_exclude_attrs']:
            if key not in self.__dict__:
                raise AttributeError('attr \'%s\' not exist' % key)
            return self.__dict__[key]
        else:
            return self._attrs[key] if key in self._attrs else None

    def __setattr__(self, key, value):
        if key.startswith('__') and key.endswith('__'):
            return super(Model, self).__setattr__(key)

        if key == '_exclude_attrs' or key in self.__dict__['_exclude_attrs']:
            self.__dict__[key] = value
        else:
            try:
                self._attrs[self._renames[key]] = value
            except KeyError:
                self._attrs[key] = value

    def contains_attr(self, key):
        return key in self._attrs

    def get_attr(self, key):
        return self.__getattr__(key)

    def set_attr(self, key, value):
        self.__setattr__(key, value)

    def remove_attr(self, key):
        return self._attrs.pop(key)

    def set_attrs(self, attrs={}):
        for key in attrs:
            self.set_attr(key, attrs[key])

    def clear_attrs(self):
        return self._attrs.clear()

    def get_attrs(self):
        return self._attrs

    def to_json(self, exclude_attrs=[]):
        attrs = self._attrs.copy()
        for exclude_attr in exclude_attrs:
            if exclude_attr in attrs:
                del attrs[exclude_attr]

        return json.dumps(attrs, ensure_ascii=False)

    def from_json(self, json_str, exclude_attrs=[]):
        attrs = json.loads(json_str)
        for exclude_attr in exclude_attrs:
            try:
                del attrs[exclude_attr]
            except KeyError:
                pass
        self._attrs.update(attrs)


class BaseModel(Model):

    def __init__(self, db_name=None, table_name=None, table_alias=None, primary_key='id'):
        super(BaseModel, self).__init__(['db_name', 'table_name', 'table_alias', 'primary_key'])
        self.db_name = db_name
        self.table_name = table_name
        self.table_alias = table_alias
        self.primary_key = primary_key

    @abstractmethod
    def get_dialect(self):
        raise NotImplementedError

    @abstractmethod
    def get_executor(self):
        raise NotImplementedError

    def save(self):
        attrs = self.get_attrs()
        columns = [column for column in attrs.keys() if column != self.primary_key]
        params = [attrs[column] for column in columns]
        sql = self.get_dialect().insert(columns=columns, table=self.table_name, primary_key=self.primary_key, sequence=self.sequence)
        primary_value = self.get_executor().insert(sql=sql, params=tuple(params))
        self.set_attr(self.primary_key, primary_value)
        return primary_value

    '''
    根据主键更新单个对象
    '''

    def update(self):
        attrs = self.get_attrs()
        columns = [column for column in attrs.keys() if column != self.primary_key]
        params = [attrs[column] for column in columns]
        params.append(attrs[self.primary_key])
        sql = self.get_dialect().update(columns=columns, table=self.table_name, alias=self.table_alias, where='`%s`= %s' % (self.primary_key, '%s'))
        return self.get_executor().update(sql=sql, params=tuple(params))

    '''
    根据主键删除单个对象
    '''

    def delete(self):
        sql = self.get_dialect().delete(table=self.table_name, where='`%s`= %s' % (self.primary_key, '%s'))
        return self.get_executor().delete(sql, tuple([self.get_attr(self.primary_key)]))

        '''
        根据主键查询单个对象
        '''

    def query(self):
        sql = self.get_dialect().select(columns=['*'], table=self.table_name, alias=self.table_alias, where='`%s`= %s' % (self.primary_key, '%s'))
        result = self.get_executor().select_first(sql=sql, params=tuple([self.get_attr(self.primary_key)]))

        self.clear_attrs()
        self.set_attrs(result.copy())
        return self
