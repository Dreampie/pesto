from pesto_example.model.example import Example
from pesto_orm.dialect.mysql.repository import MysqlBaseRepository


class ExampleRepository(MysqlBaseRepository):
    def __init__(self):
        super(ExampleRepository, self).__init__(Example)
