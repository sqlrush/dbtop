# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
日志模块 (Logger)

对 Python logging 模块的轻量封装，提供统一的日志接口。

核心功能:
    - 基于 RotatingFileHandler 的自动日志轮转（默认 10MB/文件，保留 5 个备份）
    - 统一的日志格式：时间戳(毫秒精度) - 模块名 - 级别 - 消息
    - 支持按模块创建独立的 Logger 实例（如 dbtop、emergency、data 等）
    - 提供模块级快捷函数 log.info()、log.error() 等

典型用法:
    logger = Logger(name='dbtop', log_file='dbtop_app.log', level='WARNING')
    logger.info("Monitor started")
    logger.error("Connection failed: %s", err)
"""

import logging
import logging.handlers
import os
from typing import Optional


class Logger:
    def __init__(
            self,
            name: str = "default_logger",
            log_file: Optional[str] = None,
            level: str = "INFO",
            fmt: str = "%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s - %(message)s",
            datefmt: str = "%Y-%m-%d %H:%M:%S",
            max_bytes: int = 10 * 1024 * 1024,
            backup_count: int = 5,
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level.upper())

        if not self.logger.handlers:
            formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

            if log_file:
                log_dir = os.path.dirname(log_file)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir)

                file_handler = logging.handlers.RotatingFileHandler(
                    filename=log_file,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding='utf-8'
                )
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)

    def debug(self, msg: str, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def critical(self, msg: str, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs):
        self.logger.exception(msg, *args, **kwargs)

    def set_level(self, level: str):
        self.logger.setLevel(level.upper())


default_logger = Logger()
debug = default_logger.debug
info = default_logger.info
warning = default_logger.warning
error = default_logger.error
critical = default_logger.critical
exception = default_logger.exception
