'''
orm 基础对象结构
'''
from pesto_orm.dialect.mysql.domain import mysqlExecutor
from pesto_orm.model import BaseModel


class MysqlBaseModel(BaseModel):

    def __init__(self, db_name=None, table_name=None, primary_key="id"):
        super(MysqlBaseModel, self).__init__(["db_name", "table_name", "primary_key", "from_sql"])
        self.db_name = db_name
        self.table_name = table_name
        self.primary_key = primary_key
        self.from_sql = None

    def _assembly_from_sql(self):
        if self.from_sql is None:
            self.from_sql = ("`" + self.db_name + "`." if self.db_name is not None else "") + ("`" + self.table_name + "`" if self.table_name is not None else "")
        return self.from_sql

    def save(self):
        separator, columns, value_holders, values = self._assembly_save()
        sql = "INSERT INTO " + self._assembly_from_sql() + "(" + separator.join(columns) + ") VALUES (" + separator.join(
            value_holders) + ")"

        id = mysqlExecutor.insert(sql, tuple(values))
        self.set_attr(self.primary_key, id)
        return id

    """
    根据主键更新单个对象
    """

    def update(self):
        separator, set_holders, values = self._assembly_update()
        sql = "UPDATE " + self._assembly_from_sql() + " SET " + separator.join(set_holders) + " WHERE `" + self.primary_key + "`= %s"

        return mysqlExecutor.update(sql, tuple(values))

    """
    根据主键删除单个对象
    """

    def delete(self):
        sql = "DELETE FROM " + self._assembly_from_sql() + " WHERE `" + self.primary_key + "`= %s"

        return mysqlExecutor.delete(sql, tuple([self.get_attr(self.primary_key)]))

    """
    根据主键查询单个对象
    """

    def query(self):
        sql = "SELECT * FROM " + self._assembly_from_sql() + " WHERE `" + self.primary_key + "`= %s"

        result = mysqlExecutor.select_first(sql=sql, params=tuple([self.get_attr(self.primary_key)]))

        self.clear_attrs()
        self.set_attrs(result.copy())
        return self
