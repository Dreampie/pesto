# -*- coding:utf8 -*-
import codecs
import os
import sys
import threading
from configparser import ConfigParser, NoOptionError

global root_path
root_path = None
global root_project
root_project = None
global default_config_path
default_config_path = None

lock = threading.Lock()


class Configer(object):
    __configs = {}
    __env = os.environ.get('env', 'local')

    @staticmethod
    def get_env():
        return Configer.__env

    """
    set default config path
    """

    @staticmethod
    def use_default(path):
        global default_config_path
        default_config_path = path
        return Configer.use(path)

    @staticmethod
    def use(path):
        try:
            lock.acquire()
            try:
                return Configer.__configs[path]
            except KeyError:
                _config = _Config(Configer.__env, path)
                Configer.__configs[path] = _config
                return _config
        finally:
            lock.release()

    @staticmethod
    def refresh(path):
        if path in Configer.__configs:
            Configer.use(path)

    @staticmethod
    def __init():
        global root_path
        global root_project
        global default_config_path
        if default_config_path is None or root_path is None or root_project is None:
            current_path = os.path.abspath(sys.path[0])
            Configer.__search_config(current_path)
            if default_config_path is None:
                raise RuntimeError(
                    "if use this method, must init with `Configer.use_default(path)` before import class.")

    """
    get from default path
    """

    @staticmethod
    def get(name, default_value=None):
        Configer.__init()

        config_value = Configer.use(default_config_path).get(name, default_value)
        global root_path
        global root_project
        config_value = Configer.__convert_value(config_value, root_path, root_project)
        return config_value

    @staticmethod
    def __convert_value(config_value, root_path, root_project):
        if config_value is not None:

            if isinstance(config_value, str) and config_value.startswith('$root'):
                config_value = config_value.replace('$root_project', root_project).replace('$root_path', root_path)
        return config_value

    @staticmethod
    def contains(prefix):
        Configer.__init()
        config_options = Configer.use(default_config_path).items()
        for name in config_options:
            if name.startswith(prefix):
                return True
        return False

    @staticmethod
    def list(prefix):
        items = []
        Configer.__init()
        config_options = Configer.use(default_config_path).items()
        for name in config_options:
            if name.startswith(prefix):
                items.append(name)
        return items

    @staticmethod
    def __search_config(current_path, level=1):
        global root_path
        global root_project
        global default_config_path
        file_names = os.listdir(current_path)
        if 'requirements.txt' in file_names:
            root_path = current_path
            root_project = os.path.basename(root_path)

            if default_config_path is None:
                config_path = current_path + '/config.ini'
                if os.path.exists(current_path):
                    default_config_path = config_path

        if (default_config_path is None or root_path is None) and level <= 10:
            Configer.__search_config(os.path.dirname(current_path), level=level + 1)


class _Config(object):

    def __init__(self, env, path):
        self.env = env
        self.parser = ConfigParser(os.environ)
        self.parser.read_file(codecs.open(path, 'r', 'utf8'))

    def get(self, name, default_value=None):
        try:
            return self.parser.get(self.env, name)
        except NoOptionError:
            return default_value

    def items(self):
        return self.parser.options(self.env)


app_key = Configer.get('app.key')
if app_key is None:
    raise RuntimeError('before use configer, please set app.key to init')
