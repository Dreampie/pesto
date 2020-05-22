# -*- coding:utf8 -*-
import hashlib


class HashUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def md5(content):
        md5 = hashlib.md5()
        md5.update(content)
        return md5.hexdigest()
