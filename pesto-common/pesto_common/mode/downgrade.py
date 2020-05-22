import multiprocessing
import os
import signal
import traceback

from pesto_common.log.logger_factory import LoggerFactory

logger = LoggerFactory.get_logger('mode.downgrade')
"""
:param exceptions exception class list
:param timeout 超时时间秒
:param callback  超时回调函数,如果不设置会抛出timeout异常
"""


def fallback(exceptions=[], timeout=-1, callback=None):
    def wrap(func):
        def handle(result, **kwargs):  # 真实执行原方法.
            func = kwargs['func']
            args = kwargs['args']
            kwargs = kwargs['kwargs']
            return_value = func(*args, **kwargs)
            logger.info("Handle method: " + func.__name__)
            result.append(return_value)

        def to_do(*args, **kwargs):
            manager = multiprocessing.Manager()
            new_kwargs = {'func': func, 'args': args, 'kwargs': kwargs}

            result = manager.list()

            process = multiprocessing.Process(target=handle, args=(result,), kwargs=new_kwargs)
            process.daemon = True
            try:
                timer_able = timeout > 0

                process.start()
                if timer_able:
                    process.join(timeout)
                else:
                    process.join()

                if process.is_alive():
                    __close_process(process)
                    if callback is not None:
                        logger.warning("Method execute timeout. timeout: " + str(timeout) + "s, method: " + str(func.__name__) + ", callback: " + str(callback.__name__))
                        callback()
                    else:
                        raise TimeoutError("Method execute timeout. timeout: " + str(timeout) + "s, method: " + str(func.__name__) + ", no callback")
                else:
                    return result[0]
            except Exception as e:
                if not isinstance(e, TimeoutError):
                    if process.is_alive():
                        __close_process(process)
                if e.__class__ in exceptions:
                    if callback is not None:
                        logger.warning("Method execute error. method: " + str(func.__name__) + ", callback: " + str(callback.__name__) + ", message:" + traceback.format_exc())
                        callback()
                    else:
                        raise e
                else:
                    raise e

        return to_do

    return wrap


class ProcessError(RuntimeError):
    pass


def __close_process(process):
    process.terminate()  # kill the child thread
    process.join()
    if process.is_alive():
        os.kill(process.ident, signal.SIGKILL)
        killed_pid, stat = os.waitpid(process.ident, os.WNOHANG)
        if killed_pid == 0:
            raise ProcessError("Process kill failed, pid: " + process.ident)
