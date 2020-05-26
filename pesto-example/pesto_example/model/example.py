from pesto_orm.dialect.mysql.domain import MysqlBaseModel


class Example(MysqlBaseModel):
    def __init__(self):
        super(Example, self).__init__(table_name='example', primary_key='id')
