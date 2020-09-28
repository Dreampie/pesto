# -*- coding:utf8 -*-
import datetime


class DateUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def gen_dates(start, days):
        day = datetime.timedelta(days=1)
        for i in range(days):
            yield start + day * i

    @staticmethod
    def get_dates(start=None, end=None):
        """
        获取日期列表
        :param start: 开始日期
        :param end: 结束日期
        :return:
        """
        if start is None:
            start = datetime.datetime.strptime("1970-01-01", "%Y-%m-%d")
        if end is None:
            end = datetime.datetime.now()
        if isinstance(start, str):
            start = datetime.datetime.strptime(start, "%Y-%m-%d")
        if isinstance(end, str):
            end = datetime.datetime.strptime(end, "%Y-%m-%d")

        day_count = (end - start).days
        dates = []
        if day_count > 0:
            for day in DateUtils.gen_dates(start, (end - start).days + 1):
                dates.append(day)
        else:
            dates.append(start)
        return dates

    @staticmethod
    def utc_to_beijing(utc_time, format='%Y-%m-%dT%H:%M:%S.%fZ'):
        if isinstance(utc_time, datetime.datetime):
            date = utc_time + datetime.timedelta(hours=8)
        elif isinstance(utc_time, str):
            date = datetime.datetime.strptime(utc_time, format) + datetime.timedelta(hours=8)
        else:
            raise RuntimeError('not support this type: {}'.format(type(utc_time)))
        return date


if __name__ == '__main__':
    dates = DateUtils.get_dates(start=datetime.datetime.strptime("2019-03-01", "%Y-%m-%d"),
                                end=datetime.datetime.strptime("2019-03-02", "%Y-%m-%d"))
    print(dates)
