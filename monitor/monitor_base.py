# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
监控基类模块 (Monitor Base)

所有监控面板的抽象基类，定义了监控模块的统一接口和公共行为。

核心职责:
    - 定义 refresh() / print() / parse_config() 抽象接口
    - 管理 curses pad 的创建、清屏、输出（支持 daemon 模式下无终端运行）
    - 封装 SQL 执行器和 OS 命令执行器（通过 util 工厂函数创建）
    - 提供告警上报接口 check_and_report_alarm()
    - 管理 dump_data 用于应急模块的屏幕快照持久化
    - 通过 create_logged_pad() 实现双写：同时输出到 curses pad 和 dump_data 字典
    - 会话终止功能：terminate_session() / terminate_backend()

子类实现:
    DBMonitor, OSMonitor, InsMonitor, EventMonitor, SessionMonitor, MemoryMonitor
"""

import curses
import copy
import os
import socket, subprocess, sys
import time
import threading
from abc import ABC, abstractmethod
from common import alarm, log, util
from common.config import Config
from collections import defaultdict
from datetime import datetime
from functools import wraps

class Monitor(ABC):
    def __init__(self, name, height, log_level):
        self.name = name
        self.begin_x = 0
        self.begin_y = 0
        self.height = height
        self.width = 0
        self.printer = None
        self.print_to_screen = True
        self.monitor_item = []
        self.monitor_method = []
        self.monitor_value = []
        self.monitor_width = []
        self.dump_data = defaultdict(dict)
        self.logger = log.Logger(name=name, log_file='dbtop_app.log', level=log_level)
        self.message_queue = None
        self.execute_query = util.get_execute_query(self.logger)
        self.execute_noreturn_query = util.get_execute_noreturn_query(self.logger)
        self.execute_os_command = util.get_execute_os_command(self.logger)
        self.lock = threading.Lock()

    def init(self, begin_x, begin_y, width):
        self.begin_x = begin_x
        self.begin_y = begin_y
        self.width = width
        if not Config.get("main.daemon"):
            self.printer = self.create_logged_pad(curses.newpad(self.height, self.width))
        else:
            self.printer = self.create_logged_pad(None)

    @staticmethod
    def base_path(path):
        if getattr(sys, 'frozen', None):
            basedir = sys._MEIPASS
        else:
            basedir = os.path.dirname(__file__)
            path = path.split('/')[-1]
        return os.path.join(basedir, path)

    @abstractmethod
    def parse_config(self, config_file):
        pass

    def terminate_session(self, sid, serial_num):
        command = f"ALTER SYSTEM KILL SESSION '{sid},{serial_num}' IMMEDIATE"
        self.logger.warning("Exec command: %s", command)
        self.execute_noreturn_query(command)

    def terminate_backend(self, sid):
        lookup = f"SELECT SERIAL# FROM v$session WHERE SID = {sid}"
        result = self.execute_query(lookup)
        if result and len(result) > 0:
            serial_num = result[0][0]
            command = f"ALTER SYSTEM KILL SESSION '{sid},{serial_num}' IMMEDIATE"
            self.logger.warning("Exec command: %s", command)
            self.execute_noreturn_query(command)

    @abstractmethod
    def refresh(self):
        pass

    def check_and_report_alarm(self):
        for i, item in enumerate(self.monitor_item):
            alarm.check_and_report_alarm(self.logger, item, self.monitor_value[i])

    def clear_screen(self, stdscr, clear_header=True):
        start_y = 0
        start_x = self.begin_x
        if clear_header:
            if not Config.get("main.daemon"):
                self.printer.addstr(start_y, start_x, " " * (self.width - 1), curses.color_pair(2))
            else:
                self.printer.addstr(start_y, start_x, " " * (self.width - 1), None)
            start_y += 1

        for clear_y in range(start_y, self.height):
            self.printer.addstr(clear_y, start_x, " " * (self.width - 1))

        Monitor.print(self, stdscr)
        self.clear_dump_data()

    def print(self, stdscr):
        if not self.print_to_screen:
            return

        if stdscr is not None:
            screen_height, screen_width = stdscr.getmaxyx()
            self.printer.refresh(0, 0,
                                 min(self.begin_y, screen_height - 1),
                                 min(self.begin_x, screen_width - 1),
                                 min(self.begin_y + self.height - 1, screen_height - 1),
                                 min(self.begin_x + self.width - 1, screen_width - 1))

    def create_logged_pad(self, pad):
        def addstr_with_log(y, x, text, *args):
            if pad is not None:
                pad.addstr(y, x, text, *args)
            for i, char in enumerate(text):
                self.dump_data[y][x + i] = char

        from types import SimpleNamespace
        logged_pad = SimpleNamespace()

        if pad is not None:
            for attr in dir(pad):
                if not attr.startswith('_'):
                    setattr(logged_pad, attr, getattr(pad, attr))

        logged_pad.addstr = addstr_with_log
        return logged_pad

    def get_monitor_value(self):
        return self.monitor_value

    def get_dump_data(self):
        return copy.deepcopy(self.dump_data)

    def clear_dump_data(self):
        self.dump_data.clear()

    def set_message_queue(self, queue):
        self.message_queue = queue
