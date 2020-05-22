#!/usr/bin/env python
# encoding: utf-8

from setuptools import setup, find_packages

requirements = []

setup(name='pesto-common',
      version='0.0.3',
      author='Dreampie',
      author_email='Dreampie@outlook.com',
      url='https://github.com/Dreampie/pesto',
      description='Minimalist python utils',
      long_description='Minimalist python utils',
      long_description_content_type='text/markdown',
      keywords=['minimalist', 'python', 'utils', 'pesto'],
      packages=find_packages(),
      include_package_data=True,
      install_requires=requirements,
      platforms=['all']
      )
