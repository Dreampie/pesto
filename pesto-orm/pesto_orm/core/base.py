from enum import Enum

from pesto_common.config.configer import Configer


class ExecuteMode(Enum):
    ONE_MODE = 0
    MANY_MODE = 1


class ResultMode(Enum):
    STORE_RESULT_MODE = 0
    USE_RESULT_MODE = 1


class CursorMode(Enum):
    CURSOR_MODE = 0
    DICT_CURSOR_MODE = 1


db_config = {}
if Configer.contains('db.'):
    db_config = {
        'show_sql': bool(Configer.get('db.show_sql', False)),
        'user': Configer.get('db.user'),
        'password': Configer.get('db.password'),
        'host': Configer.get('db.host'),
        'port': Configer.get('db.port'),
        'database': Configer.get('db.database'),
        'core_size': Configer.get('db.core_size', 20),
        'max_size': Configer.get('db.max_size', 100),
        'raise_on_warnings': bool(Configer.get('db.raise_on_warnings', False)),
        'charset': Configer.get('db.charset', 'utf8mb4'),
        'connection_timeout': int(Configer.get('db.connection_timeout', 180)),
        'autocommit': bool(Configer.get('db.autocommit', True)),
    }
