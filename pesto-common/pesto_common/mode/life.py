import errno
import os


def singleton(cls):
    _instances = {}

    def inner():
        try:
            return _instances[cls]
        except KeyError:
            _cls = cls()
            _instances[cls] = _cls
            return _cls

    return inner


def multiton(cls):
    _instances = {}

    def exist(pid):
        """Check whether pid exists in the current process table.
        UNIX only.
        """
        if pid < 0:
            return False
        if pid == 0:
            # According to "man 2 kill" PID 0 refers to every process
            # in the process group of the calling process.
            # On certain systems 0 is a valid PID but we have no way
            # to know that in a portable fashion.
            raise ValueError('invalid PID 0')
        try:
            os.kill(pid, 0)
        except OSError as err:
            if err.errno == errno.ESRCH:
                # ESRCH == No such process
                return False
            elif err.errno == errno.EPERM:
                # EPERM clearly means there's a process to deny access to
                return True
            else:
                # According to "man 2 kill" possible error values are
                # (EINVAL, EPERM, ESRCH)
                raise
        else:
            return True

    def inner():
        try:
            return _instances[cls][os.getpid()]
        except KeyError:
            _cls = cls()
            if cls not in _instances:
                _instances[cls] = {}
            _instances[cls][os.getpid()] = _cls

            # 进程维度  清理对象
            if len(_instances[cls]) > 8:
                del_pids = set()
                for pid in _instances[cls]:
                    if not exist(pid):
                        del_pids.add(pid)

                _instances[cls] = {pid: _instances[cls][pid] for pid in _instances[cls] if pid not in del_pids}
            return _cls

    return inner
