# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from setuptools import setup, find_packages

setup(
    name='dbtop',
    version='1.0.0',
    author='sqlrush',
    author_email='',
    description='Oracle Database Real-time Monitor',
    packages=find_packages(),
    package_data={'tool': ['dbtop.cfg'], 'monitor': ['*.cfg']},
    include_package_data=True,
    entry_points={
        'console_scripts': ['dbtop=tool.dbtop:main'],
    },
    install_requires=[],
    python_requires='>=3.6',
)
