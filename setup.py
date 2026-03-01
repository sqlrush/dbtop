# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
dbtop 安装配置 (Setup)

通过 pip install . 安装后，可直接使用 'dbtop' 命令启动。
entry_points 将 tool.dbtop:main 注册为 console_scripts。
package_data 确保 dbtop.cfg 和 monitor/*.cfg 配置文件随包安装。
"""

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
