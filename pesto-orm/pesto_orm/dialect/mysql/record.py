from pesto_orm.dialect.mysql.domain import MysqlBaseRepository


class MysqlRecordRepository(MysqlBaseRepository):

    def __init__(self, db_name=None, table_name=None, table_alias=None, primary_key='id', sequence=None, model_class=None):
        MysqlBaseRepository.__init__(self, model_class=model_class)
        self.db_name = db_name
        self.table_name = table_name
        self.table_alias = table_alias
        self.primary_key = primary_key
        self.sequence = sequence
