import logging
import os
import threading
import time
from logging.handlers import TimedRotatingFileHandler

from pesto_common.config.configer import Configer


def doRollover(self):
    """
    do a rollover; in this case, a date/time stamp is appended to the filename
    when the rollover happens.  However, you want the file to be named for the
    start of the interval, not the current time.  If there is a backup count,
    then we have to get a list of matching filenames, sort them and remove
    the one with the oldest suffix.
    """
    if self.stream:
        self.stream.close()
        self.stream = None
    # get the time that this sequence started at and make it a TimeTuple
    currentTime = int(time.time())
    dstNow = time.localtime(currentTime)[-1]
    t = self.rolloverAt - self.interval
    if self.utc:
        timeTuple = time.gmtime(t)
    else:
        timeTuple = time.localtime(t)
        dstThen = timeTuple[-1]
        if dstNow != dstThen:
            if dstNow:
                addend = 3600
            else:
                addend = -3600
            timeTuple = time.localtime(t + addend)
    dfn = self.baseFilename.replace('.log', '') + '.' + time.strftime(self.suffix, timeTuple) + '.log'
    if os.path.exists(dfn):
        os.remove(dfn)
    # Issue 18940: A file may not have been created if delay is True.
    if os.path.exists(self.baseFilename):
        os.rename(self.baseFilename, dfn)
    if self.backupCount > 0:
        for s in self.getFilesToDelete():
            os.remove(s)
    if not self.delay:
        self.stream = self._open()
    newRolloverAt = self.computeRollover(currentTime)
    while newRolloverAt <= currentTime:
        newRolloverAt = newRolloverAt + self.interval
    # If DST changes and midnight or weekly rollover, adjust for this.
    if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
        dstAtRollover = time.localtime(newRolloverAt)[-1]
        if dstNow != dstAtRollover:
            if not dstNow:  # DST kicks in before next rollover, so we need to deduct an hour
                addend = -3600
            else:  # DST bows out before next rollover, so we need to add an hour
                addend = 3600
            newRolloverAt += addend
    self.rolloverAt = newRolloverAt


logging.handlers.TimedRotatingFileHandler.doRollover = doRollover


def emit(self, record):
    try:
        msg = self.format(record)
        stream = self.stream
        fs = "%s\n"
        try:
            stream.write(fs % msg)
        except UnicodeError:
            try:
                stream.write(fs % msg.encode("UTF-8"))
            except UnicodeDecodeError:
                stream.write(fs % msg.decode('ISO-8859-1'))
        self.flush()
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        self.handleError(record)


logging.StreamHandler.emit = emit

lock = threading.Lock()

global loggerFactory
loggerFactory = None  # type: LoggerFactory


class LoggerFactory(object):

    def __init__(self, filename, level=logging.INFO, backup_count=7):
        """
        :param filename:
        :param loggername:
        """
        # self.filename = filename.format(str(time.strftime("%Y-%m-%d", time.localtime())))
        self.filename = filename
        self.level = level
        self.backup_count = backup_count
        self.suffix = '.log'
        self.prefix = self.filename.replace(self.suffix, '')

    def __get_logger(self, logger_name=None, level=None, format="%(asctime)s [%(threadName)s] %(levelname)s  %(name)s - %(message)s"):
        # logging.basicConfig(level=level, format=format)

        if level is None:
            level = self.level

        if not logging.root.handlers:
            # file_handler = logging.FileHandler(self.filename, 'a+')
            # file_handler.setFormatter(logging.Formatter(format))
            # logging.root.addHandler(file_handler)
            logging.root.setLevel(self.level)

            formatter = logging.Formatter(format)

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            console_handler.encoding = 'utf-8'
            logging.root.addHandler(console_handler)

            file_handler = TimedRotatingFileHandler(filename=self.filename, when='midnight', interval=1, backupCount=self.backup_count, encoding='utf-8')
            file_handler.setFormatter(formatter)
            file_handler.suffix = "%Y-%m-%d"
            file_handler.extMatch = r"^\d{4}-\d{2}-\d{2}$"
            logging.root.addHandler(file_handler)

        logger = logging.getLogger(logger_name)
        logger.level = level
        return logger

    def get_filename(self):
        return self.filename

    @staticmethod
    def get_logger(logger_name):
        LoggerFactory.__init_logger_factory()

        global loggerFactory
        logger = loggerFactory.__get_logger(logger_name=logger_name)
        return logger

    @staticmethod
    def __init_logger_factory():

        try:
            lock.acquire()
            global loggerFactory
            if loggerFactory is None:
                log_path = Configer.get('log.path')
                if log_path is None:
                    raise RuntimeError('log.path not found')
                log_dir = os.path.dirname(log_path)
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)

                log_level = Configer.get('log.level')
                if log_level is None:
                    raise RuntimeError('log.level not found')
                log_level = log_level.upper() if log_level is not None else ''
                if log_level == 'DEBUG':
                    level = logging.DEBUG
                elif log_level == 'INFO':
                    level = logging.INFO
                elif log_level == 'WARNING':
                    level = logging.WARNING
                elif log_level == 'WARN':
                    level = logging.WARN
                elif log_level == 'ERROR':
                    level = logging.ERROR
                else:
                    raise RuntimeError('log level error: {}'.format(log_level))

                log_backup_count = Configer.get('log.backup_count')
                if log_backup_count is None:
                    log_backup_count = 7
                loggerFactory = LoggerFactory(log_path, level, log_backup_count)

                logger = loggerFactory.__get_logger(logger_name='logger.factory')
                env = os.environ.get('env', 'local')
                logger.info('Start env: ' + env)
                logger.info('Log file path: {}, default level: {}'.format(loggerFactory.get_filename(), log_level))
        finally:
            lock.release()
