import re
from abc import ABCMeta, abstractmethod

from pesto_orm.core.error import DBError


class Dialect(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_db_type(self):
        raise NotImplementedError

    @abstractmethod
    def select(self, columns, table, alias, where):
        raise NotImplementedError

    @abstractmethod
    def insert(self, columns, table, primary_key, sequence):
        raise NotImplementedError

    @abstractmethod
    def delete(self, table, where):
        raise NotImplementedError

    @abstractmethod
    def update(self, columns, table, alias, where):
        raise NotImplementedError

    @abstractmethod
    def count(self, table, alias, where):
        raise NotImplementedError

    @abstractmethod
    def count_with(self, sql):
        raise NotImplementedError

    @abstractmethod
    def paginate(self, columns, table, alias, where, page_number, page_size):
        raise NotImplementedError

    @abstractmethod
    def paginate_with(self, sql, page_number, page_size):
        raise NotImplementedError


class DefaultDialect(Dialect):
    __metaclass__ = ABCMeta

    select_pattern = re.compile(r'^\s*SELECT\s+', re.M | re.I)
    order_pattern = re.compile(r'\s+ORDER\s+BY\s+', re.M | re.I)
    group_pattern = re.compile(r'\s+GROUP\s+BY\s+', re.M | re.I)
    having_pattern = re.compile(r'\s+HAVING\s+', re.M | re.I)
    select_single_pattern = re.compile(r'^\s*SELECT\s+((COUNT)\([\s\S]*\)\s*,?)+((\s*)|(\s+FROM[\s\S]*))?$', re.M | re.I)

    def get_alias(self, alias):
        if alias is not None and len(alias.strip()) > 0:
            alias = ' ' + alias
        else:
            alias = ''

        return alias

    def get_prefix(self, alias, columns=[]):
        if alias is not None and len(alias.strip()) > 0 and len(columns) > 0:
            new_columns = []
            for column in columns:
                if column.contains('.'):
                    return columns
                else:
                    new_columns.append(alias + '.' + column)

            return new_columns
        else:
            return columns

    def get_columns(self, alias, columns):

        if columns is not None and len(columns) > 0:
            return ', '.join(self.get_prefix(alias=alias, columns=columns))
        else:
            return '*'

    def get_where(self, where):
        if where is None or len(where) == 0 or len(where.strip()) == 0:
            return ''
        if not where.startswith(' '):
            where = ' ' + where

        m = re.match(DefaultDialect.order_pattern, where)
        if m is not None:
            span = m.span()
            if span[0] == 0:
                return where
        m = re.match(DefaultDialect.group_pattern, where)
        if m is not None:
            span = m.span()
            if span[0] == 0:
                return where
        m = re.match(DefaultDialect.having_pattern, where)
        if m is not None:
            span = m.span()
            if span[0] == 0:
                return where
        return ' WHERE' + where

    def get_placeholders(self, count):
        sql = ''
        for i in range(count):

            if i == 0:
                sql += '%s'
            else:
                sql += ',%s'
        return sql

    def select(self, columns, table, alias, where):
        columns = self.get_columns(alias, columns)
        where = self.get_where(where=where)
        alias = self.get_alias(alias=alias)
        return 'SELECT %s FROM %s%s%s' % (columns, table, alias, where)

    def insert(self, columns, table, primary_key, sequence):
        sql = 'INSERT INTO %s (' % table
        in_sequence = True if primary_key is not None and len(primary_key) > 0 and sequence is not None and len(sequence) > 0 else False
        if in_sequence:
            sql += '%s,' % primary_key
        sql += ', '.join(columns)
        sql += ') VALUES ('
        if in_sequence:
            sql += ','
        sql += self.get_placeholders(len(columns))
        sql += ')'
        return sql

    def delete(self, table, where):
        where = self.get_where(where=where)
        return 'DELETE FROM %s%s' % (table, where)

    def update(self, columns, table, alias, where):
        if columns is None or len(columns) == 0:
            raise DBError('Could not found columns to update.')

        where = self.get_where(where=where)
        return 'UPDATE %s%s SET %s%s%s' % (table, self.get_alias(alias), '=%s, '.join(self.get_prefix(alias, columns)), '=%s', where)

    def count(self, table, alias, where):
        where = self.get_where(where=where)
        return 'SELECT COUNT(*) FROM %s%s%s' % (table, self.get_alias(alias), self.get_where(where))

    def count_with(self, sql):
        span = re.match(DefaultDialect.order_pattern, sql).span()
        if span[1] > sql.rindex(')'):
            sql.substring(0, span[0])
        return 'SELECT COUNT(*) FROM (%s) count_alias' % sql

    def paginate(self, columns, table, alias, where, page_number, page_size):
        return self.paginate_with(self.select(columns, table, alias, where), page_number, page_size)
