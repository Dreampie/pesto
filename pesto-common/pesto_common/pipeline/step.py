#!/usr/bin/env python
# encoding: utf-8
from abc import abstractmethod, ABCMeta

from pesto_common.log.logger_factory import LoggerFactory
from pesto_orm.model import BaseModel

logger = LoggerFactory.get_logger("pipeline.step")


class PipelineError(RuntimeError):
    pass


class PipelineStepData(BaseModel):
    pass


class PipelineStep:
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, data={}, next_step=None):
        if isinstance(data, PipelineStepData):
            self.data = data
        else:
            self.data = PipelineStepData(init_attrs=data)  # 执行需要的数据
        self.result = PipelineStepData()  # 执行的结果数据
        self.next_step = next_step  # 下一步执行内容
        self.index = 1
        self.exit = False

    def start(self):
        # 前置逻辑 如: 数据处理
        self._before()
        # 核心步骤
        self._run()
        # 后置逻辑 如: 释放资源
        self._after()
        self.__pass_data()
        if not self.exit:
            if self.next_step is not None:
                return self.next_step.start()
        return True

    def set_next_step(self, next_step):
        if self.next_step is None:
            next_step.index = self.index + 1
            self.next_step = next_step
        else:
            self.next_step.set_next_step(next_step)
        return self

    def __pass_data(self):
        if self.next_step is not None:
            for key in self.result.get_attrs():
                if self.next_step.data.contains_attr(key):
                    raise PipelineError('Next step contains key for data, duplicate key: ' + key)
                self.next_step.data.set_attr(key, self.result.get_attr(key))
            # clean data
            self.data.clear_attrs()
            self.result.clear_attrs()

    def _before(self):
        pass

    @abstractmethod
    def _run(self):
        raise NotImplementedError

    def _after(self):
        pass
