# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
应急预案主控模块 (Emergency Main)

协调所有应急检测子模块的运行、展示和持久化。

核心职责:
    - emergency_main(): 每个刷新周期调用，将当前监控数据分发给 8 个子模块并行分析
    - emergency_print(): 在终端底部展示已触发的应急信息（红色高亮）
    - emergency_persist(): 保存触发应急时的屏幕快照到日志文件
    - 提供交互命令入口：用户在应急面板按 'k' 可执行子模块的处理命令

8 个应急子模块:
    1. PlanChange — 执行计划变更检测
    2. MemoryFull — SGA/PGA 内存异常检测
    3. IOFull — I/O 满载检测
    4. CPUFull — CPU 满载检测
    5. SessionsFull — 活跃会话数过多检测
    6. ConnectionsFull — 连接数过多检测
    7. PerformanceJitter — 性能抖动检测
    8. SlowSQL — 慢 SQL 检测

运行机制:
    - 各子模块在独立线程中并行执行 analyze()
    - 任一子模块触发应急 → emergency_triggered = True → 终端显示应急面板
    - 应急触发时自动开始持久化屏幕快照，持续 snapshot_persist_number 个采样周期
"""

import copy
import curses
import threading
import traceback

from collections import defaultdict
from datetime import datetime

from common import log, util
from common.config import Config
from .persist import Persist
from .mem_persist import MemPersist
from .connections_full import ConnectionsFull
from .cpu_full import CPUFull
from .io_full import IOFull
from .memory_full import MemoryFull
from .performance_jitter import PerformanceJitter
from .plan_change import PlanChange
from .slow_sql import SlowSQL
from .sessions_full import SessionsFull

EMERGENCY_WINDOW_HEIGHT = 20


class EmergencyMain:
    def __init__(self, begin_x, begin_y, width):
        # init printer
        self.begin_x = begin_x
        self.begin_y = begin_y
        self.width = width
        self.height = EMERGENCY_WINDOW_HEIGHT
        self.printer = None
        if not Config.get("main.daemon"):
            self.printer = curses.newpad(self.height, self.width)
        # init logger
        self.logger = log.Logger(name='emergency', log_file='dbtop_emergency_run.log', level='INFO')
        self.logger.info("DBTOP emergency module start running.")
        # init persist module
        self.persist = MemPersist()
        # init each emergency module
        self.plan_change_module = PlanChange(self.logger, self.persist)
        self.performance_jitter_module = PerformanceJitter(self.logger, self.persist)
        self.slow_sql_module = SlowSQL(self.logger, self.persist)
        self.cpu_full_module = CPUFull(self.logger, self.persist)
        self.io_full_module = IOFull(self.logger, self.persist)
        self.sessions_full_module = SessionsFull(self.logger, self.persist)
        self.connections_full_module = ConnectionsFull(self.logger, self.persist)
        self.memory_full_module = MemoryFull(self.logger, self.persist)
        self.emergency_module_array = [
            self.plan_change_module,
            self.memory_full_module,
            self.io_full_module,
            self.cpu_full_module,
            self.sessions_full_module,
            self.connections_full_module,
            self.performance_jitter_module,
            self.slow_sql_module
        ]
        # init value
        self.curr_snap_id = 0
        self.curr_snap_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.curr_full_session = None
        # init dump data dict, key: snap_id, value: dump_data
        self.dump_data_dict = {}
        self.emer_value = []
        self.emer_dump_data = defaultdict(dict)
        self.emergency_triggered = False

    def emergency_main(self, monitors_value, full_session):
        """
        Main code entry for emergency processing
            :param monitors_value: current monitor value
            :param full_session: current full session data, obtained from the session monitor
        """
        snap_id_result = self.persist.get_snap_id()
        if snap_id_result is None:
            self.logger.error("Query snap id failed.")
            return

        self.curr_snap_id = snap_id_result[0][0]
        self.curr_snap_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.curr_full_session = full_session

        # multi threads analyze
        analyze_threads = []
        for module in self.emergency_module_array:
            module.emergency_triggered = False
            module.emergency_info = []
            module.emergency_sql_ids = []
            module.emergency_pids = []

            module.curr_snap_id = self.curr_snap_id
            module.curr_snap_ts = self.curr_snap_ts

            module.curr_db = monitors_value.get('db')
            module.curr_os = monitors_value.get('os')
            module.curr_instance = monitors_value.get('instance')
            module.curr_event = monitors_value.get('event')
            module.curr_session = monitors_value.get('session')
            module.full_session = full_session
            module.curr_memory = monitors_value.get('memory')

            thread = threading.Thread(target=util.refresh_analyze_wrapper, args=(module.logger, module.name, module.analyze))
            thread.start()
            analyze_threads.append(thread)

        for thread in analyze_threads:
            thread.join()

    def get_trigger_emergency_sql_ids(self):
        return self.plan_change_module.emergency_sql_ids

    def get_trigger_emergency_pids(self):
        module_array = [
            self.io_full_module,
            self.cpu_full_module,
            self.sessions_full_module
        ]
        for module in module_array:
            if module.emergency_triggered:
                return module.emergency_pids
        return []

    def get_memory_full_type(self):
        return self.memory_full_module.memory_full_type

    def emergency_handle_command_entry(self, stdscr, command):
        cursor_y, cursor_x = stdscr.getyx()
        selected_index = cursor_y - self.begin_y - 1
        if (selected_index <= 1) or (selected_index + 1 > len(self.emer_value)):
            return

        module = self.emer_value[selected_index][0]
        value = self.emer_value[selected_index][1]
        visible = self.emer_value[selected_index][2]
        if visible:
            module.handle_emergency_command(stdscr, command, value)

    def emergency_print_entry(self, stdscr):
        # clear screen
        start_y = 0
        start_x = self.begin_x
        if stdscr is not None:
            for clear_y in range(start_y, self.height):
                self.printer.addstr(clear_y, start_x, " " * (self.width - 1))
            # print to screen
            self.emergency_print_to_screen(stdscr)

        # clear data
        self.emer_value.clear()
        self.emer_dump_data.clear()

        self.emergency_triggered = False
        for module in self.emergency_module_array:
            if module.emergency_triggered:
                self.emergency_triggered = True
                break

        if not self.emergency_triggered:
            self.emergency_print_to_screen(stdscr)
            return

        cursor_y = 0
        cursor_x = 0
        if stdscr is not None:
            cursor_y, cursor_x = stdscr.getyx()

        def print_and_save(print_to_screen, curr_module, pos_y, pos_x, text, attr):
            if print_to_screen and attr is None:
                attr = curses.color_pair(1)
            # check selected line
            if print_to_screen and (self.begin_y + pos_y + 1 == cursor_y):
                attr = attr | curses.A_REVERSE
                self.printer.addstr(start_y, start_x, " " * (self.width - 1), curses.color_pair(2))
                # fix printed selected line not correct format
                self.emergency_print_to_screen(stdscr)

            if print_to_screen:
                self.printer.addstr(pos_y, pos_x, text, attr)

            self.emer_value.append((curr_module, text, print_to_screen))
            for i, char in enumerate(text):
                self.emer_dump_data[pos_y][pos_x + i] = char

        # print header
        start_x = self.begin_x
        start_y = 0

        print_to_screen_flag = False
        if stdscr is not None:
            print_to_screen_flag = True
            self.printer.addstr(start_y, start_x, " " * (self.width - 1), curses.color_pair(2))
            print_and_save(print_to_screen_flag, None, start_y, start_x, "EMERGENCY TRIGGERED", curses.color_pair(4))
        else:
            print_and_save(print_to_screen_flag, None, start_y, start_x, "EMERGENCY TRIGGERED", None)

        # print emergency info
        start_y = 1
        for module in self.emergency_module_array:
            if not module.emergency_triggered:
                continue

            if start_y == EMERGENCY_WINDOW_HEIGHT:
                break

            # print module header
            print_and_save(print_to_screen_flag, module, start_y, start_x, module.header, None)
            start_y += 1

            for info in module.emergency_info:
                if len(info) != 0:
                    self.logger.warning("%s", info)

                print_and_save(print_to_screen_flag, module, start_y, start_x, info, None)
                start_y += 1

            # only print one emergency info
            print_to_screen_flag = False

            print_and_save(print_to_screen_flag, module, start_y, start_x, "", None)

        # print to screen
        self.emergency_print_to_screen(stdscr)

    def emergency_print(self, stdscr):
        try:
            self.emergency_print_entry(stdscr)
        except Exception as e:
            self.logger.error("Emergency Print Exception Traceback:\n%s",
                              traceback.format_exc())

    def emergency_print_to_screen(self, stdscr):
        if stdscr is not None:
            screen_height, screen_width = stdscr.getmaxyx()
            self.printer.refresh(0, 0,
                                 min(self.begin_y, screen_height - 1),
                                 min(self.begin_x, screen_width - 1),
                                 min(self.begin_y + self.height - 1, screen_height - 1),
                                 min(self.begin_x + self.width - 1, screen_width - 1))

    def emergency_persist(self, monitors_dump_data):
        if not self.curr_full_session:
            return

        # save data for the most recent snapshots
        if len(self.dump_data_dict) == Config.get("emergency.max_snapshot_number"):
            min_key = min(self.dump_data_dict.keys())
            del self.dump_data_dict[min_key]

        # append emergency print data
        monitors_dump_data.append(copy.deepcopy(self.emer_dump_data))
        self.dump_data_dict[self.curr_snap_id] = monitors_dump_data

        for module in self.emergency_module_array:
            super(type(module), module).persist(self.dump_data_dict)
