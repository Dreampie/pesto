#!/usr/bin/env python
# encoding: utf-8

from pesto_common.pipeline.step import PipelineStep
from pesto_orm.dialect.mysql.domain import MysqlBaseRepository


class MysqlPipelineRepository(PipelineStep, MysqlBaseRepository):

    def __init__(self, db_name=None, table_name=None, primary_key='id', sql='', data={}, next_step=None, model_class=None, yield_able=True):
        MysqlBaseRepository.__init__(self, model_class=model_class)
        PipelineStep.__init__(self, data=data, next_step=next_step)
        self.db_name = db_name
        self.table_name = table_name
        self.primary_key = primary_key
        self.sql = sql
        self.yield_able = yield_able

    def _run(self):
        self.result.rows = self.query(sql=self.sql, yield_able=self.yield_able)
