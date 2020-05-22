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


class BaseModel(object):
    __metaclass__ = ABCMeta

    def __init__(self, exclude_attrs=[], renames={}, init_attrs={}):
        '''
        init  model
        :param exclude_attrs: 不作为对象属性的值，比如：orm中的table_name 等一些meta信息
        :param renames: 属性重命名，原本 a=5，可以重命名为 b=5, 用于纠正一些原生对象的不规范属性名称
        '''
        exclude_attrs.append("_attrs")
        exclude_attrs.append("_renames")

        news_exclude_attrs = list(set(exclude_attrs))
        news_exclude_attrs.sort(key=exclude_attrs.index)
        self._exclude_attrs = news_exclude_attrs
        self._attrs = {}
        self._renames = renames

        self.set_attrs(init_attrs)

    def __getattr__(self, key):
        if key.startswith('__') and key.endswith('__'):
            return super(BaseModel, self).__getattribute__(key)

        if key == "_exclude_attrs" or key in self.__dict__["_exclude_attrs"]:
            if key not in self.__dict__:
                raise AttributeError('attr \'%s\' not exist' % key)
            return self.__dict__[key]
        else:
            return self._attrs[key] if key in self._attrs else None

    def __setattr__(self, key, value):
        if key.startswith('__') and key.endswith('__'):
            return super(BaseModel, self).__setattr__(key)

        if key == "_exclude_attrs" or key in self.__dict__["_exclude_attrs"]:
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

    """
    封装save的语句
    """

    def _assembly_save(self):
        value_holders = []
        columns = []
        values = []
        attrs = self.get_attrs()
        for column in attrs:
            if column != self.primary_key:
                value_holders.append("%s")
                columns.append("`" + column + "`")
                values.append(attrs[column])
        separator = ", "
        return separator, columns, value_holders, values

    """
    封装update的语句
    """

    def _assembly_update(self):
        set_holders = []
        values = []
        attrs = self.get_attrs()
        for column in attrs:
            if column != self.primary_key:
                set_holders.append("`" + column + "`= %s")
                values.append(attrs[column])

        values.append(attrs[self.primary_key])
        separator = ", "
        return separator, set_holders, values

    """
    保存当前对象
    """

    @abstractmethod
    def save(self):
        raise NotImplementedError

    """
    根据主键更新单个对象
    """

    @abstractmethod
    def update(self):
        raise NotImplementedError

    """
    根据主键删除单个对象
    """

    @abstractmethod
    def delete(self):
        raise NotImplementedError

    """
    根据主键查询单个对象
    """

    @abstractmethod
    def query(self):
        raise NotImplementedError
