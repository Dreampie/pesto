# -*- coding:utf8 -*-

class ReflectUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def create_instance(module, class_name, *args, **kwargs):
        module_meta = __import__(module, globals(), locals(), [class_name])
        class_meta = getattr(module_meta, class_name)
        obj = class_meta(*args, **kwargs)
        return obj
