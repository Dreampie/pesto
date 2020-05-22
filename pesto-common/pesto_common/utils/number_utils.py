# -*- coding:utf8 -*-

class NumberUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def is_number(s):
        try:
            float(s)
            return True
        except (TypeError, ValueError):
            pass

        try:
            import unicodedata
            unicodedata.numeric(s)
            return True
        except (TypeError, ValueError):
            pass

        return False
