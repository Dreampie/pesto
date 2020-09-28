# -*- coding:utf8 -*-
import csv
import gzip
import os
import pickle
import tarfile


class FileUtils(object):
    def __init__(self):
        pass

    @staticmethod
    def save_dict(data, file_path):
        if len(data) > 0:
            field_names = data[0].keys()
            file_dir = os.path.dirname(file_path)
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            with open(file_path, 'w', newline='', encoding='utf-8-sig') as _f:
                writer = csv.DictWriter(_f, fieldnames=field_names, dialect='excel')
                writer.writeheader()
                for row in data:
                    writer.writerow(row)

    @staticmethod
    def clean_file(clean_file_path):
        if os.path.exists(clean_file_path):
            with open(clean_file_path, 'r+') as _file:
                _file.truncate()

    '''
    add file
    '''

    @staticmethod
    def add_to_file(add_file_path, item):
        with open(add_file_path, 'ab') as _file:
            _file.write(item)

    '''
    read file
    '''

    @staticmethod
    def __read_file(read_file_path):
        if os.path.exists(read_file_path):
            with open(read_file_path, 'rb') as _file:
                for line in _file:
                    yield line
        else:
            raise IOError('the file [{}] is not exist!'.format(read_file_path))

    @staticmethod
    def read_file_lines(read_file_path):
        lines = FileUtils.__read_file(read_file_path)
        if getattr(lines, '__iter__', None):
            return lines
        else:
            raise IOError('unknown error for read lines.')

    '''
    read gz file
    '''

    @staticmethod
    def __read_gz_file(read_file_path):
        if os.path.exists(read_file_path):
            with gzip.open(read_file_path, 'rb') as _file:
                for line in _file:
                    yield line
        else:
            raise IOError('the file [{}] is not exist!'.format(read_file_path))

    @staticmethod
    def read_gz_file_lines(read_file_path):
        lines = FileUtils.__read_gz_file(read_file_path)
        if getattr(lines, '__iter__', None):
            return lines
        else:
            raise IOError('unknown error for read lines.')

    @staticmethod
    def pack_gz(filename, source_dir):
        with tarfile.open(filename, "w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))

    @staticmethod
    def unpack_gz(filename, dest_dir):
        with tarfile.open(filename, "r:gz") as tar:
            tar.extractall(path=dest_dir)

    '''
    add pickel
    '''

    @staticmethod
    def add_to_pickle(add_file_path, item):
        with open(add_file_path, 'ab') as _file:
            pickle.dump(item, _file, pickle.HIGHEST_PROTOCOL)

    '''
    read pickel
    '''

    @staticmethod
    def read_pickle(read_file_path):
        if os.path.exists(read_file_path):
            with open(read_file_path, 'rb') as _file:
                try:
                    while True:
                        yield pickle.load(_file)
                except EOFError:
                    pass
        else:
            raise IOError('the file [{}] is not exist!'.format(read_file_path))

    @staticmethod
    def read_pickle_lines(read_file_path):
        lines = FileUtils.read_pickle(read_file_path)
        if getattr(lines, '__iter__', None):
            return lines
        else:
            raise IOError('unknown error for read lines.')


def main():
    FileUtils.clean_file('/Users/dreampie/PycharmProjects/dialog-mining/data/reply/standard_replies_1.txt')


if __name__ == '__main__':
    main()
