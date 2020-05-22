# -*- coding:utf8 -*-

def reraise(exception, traceback=None):
    if exception.__traceback__ is not traceback:
        raise exception.with_traceback(traceback)
    else:
        raise exception


class BaseError(Exception):
    """
    数据库相关异常
    """

    def __init__(self, cause=None, key=None, message=None, sql=None, params=None):
        self.cause = cause
        self.key = key
        self.message = message
        self.sql = sql
        self.params = params

        if key is None:
            if hasattr(cause, 'key'):
                self.key = cause.key
            if hasattr(cause, 'errno'):
                self.key = cause.errno
            if hasattr(cause, 'error_no'):
                self.key = cause.error_no
            if (self.key is None) & hasattr(cause, 'status'):
                self.key = cause.status

        self.key = '0' if self.key is None else str(self.key)

        if message is None:
            if hasattr(cause, 'msg'):
                self.message = cause.msg
            if (self.message is None) & hasattr(cause, 'message'):
                self.message = cause.message

        self.message = str(self.cause) if self.message is None else self.message

    def __str__(self):
        out_sql = ''
        if self.sql is not None:
            if isinstance(self.params, list):
                log_params = self.params[:5]
                out_sql = "Error Sql: {}, params: {}\n".format(self.sql, ', \n'.join([str(param_tuple) for param_tuple in log_params]))
            else:
                out_sql = "Error Sql: {}, params: {}".format(self.sql, self.params)

        return '{}: \"{}\"\n{}'.format(self.key, self.message, out_sql)


class DBError(BaseError):
    pass
