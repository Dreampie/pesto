# -*- coding:utf8 -*-
import socket
from contextlib import closing

from log.logger_factory import LoggerFactory

logger = LoggerFactory.get_logger('utils.ip_utils')


class IpUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def get_host_ip():
        with closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]

        return ip
