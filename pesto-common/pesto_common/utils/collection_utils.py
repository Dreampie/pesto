# encoding:utf-8
from collections import Callable, OrderedDict


class CollectionUtils(object):
    __caches = {}

    def __init__(self):
        pass

    @staticmethod
    def split(data=[], num=1):
        length = len(data)
        split_size = int(length / num)
        split_result = []
        if split_size * num == length:
            for i in range(0, length, split_size):
                split_result.append(data[i:i + split_size])
        else:
            over_size = length - num * split_size
            start = over_size * (split_size + 1)

            for i in range(0, start, split_size + 1):
                split_result.append(data[i:i + split_size + 1])

            for j in range(start, length, split_size):
                split_result.append(data[j:j + split_size])

        return split_result

    @staticmethod
    def drop_duplicates(data):
        """
        remove duplicates
        :param data:
        :return:
        """
        if len(data) > 1:
            data.sort()
            result = [data[0]]
            for i in range(1, len(data)):
                if data[i] != result[- 1]:
                    result.append(data[i])
            return result
        return data

    @staticmethod
    def difference(data1, data2):
        """
        element in data1 and not in data2
        :param data1:
        :param data2:
        :return:
        """
        l1 = len(data1)
        l2 = len(data2)
        if l1 > 0 and l2 > 0:
            data1.sort()
            data2.sort()

            result = []
            i1 = 0
            i2 = 0
            while i1 < l1 and i2 < l2:
                if data1[i1] < data2[i2]:
                    result.append(data1[i1])
                    i1 += 1
                elif data1[i1] > data2[i2]:
                    i2 += 1
                else:
                    i1 += 1

            if i1 < l1:
                result.extend(data1[i1:])

            return result
        return data1

    @staticmethod
    def search(data, x):
        """
        有序列表查找
        :param data:
        :param x:
        :return:
        """
        l = len(data)
        lo = 0
        hi = l
        while lo < hi:
            mid = (lo + hi) // 2
            if data[mid] < x:
                lo = mid + 1
            else:
                hi = mid
        return lo if lo < l and data[lo] == x else -1

    @staticmethod
    def insert(data, x):
        """
        有序列表插入
        :param data:
        :param x:
        :return:
        """
        lo = 0
        hi = len(data)
        while lo < hi:
            mid = (lo + hi) // 2
            if data[mid] < x:
                lo = mid + 1
            else:
                hi = mid
        data.insert(lo, x)
        return lo


class DefaultOrderedDict(OrderedDict):
    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and
                not isinstance(default_factory, Callable)):
            raise TypeError('first argument must be callable')
        OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))

    def __repr__(self):
        return 'DefaultOrderedDict(%s, %s)' % (self.default_factory,
                                               OrderedDict.__repr__(self))
