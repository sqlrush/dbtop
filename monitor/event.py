# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
等待事件监控模块 (Event Monitor)

实时采集 Oracle 等待事件和 DB CPU，按时间占比排序展示 Top N 等待事件。

监控指标（每行一个等待事件）:
    - EVENT: 等待事件名称（首行固定显示 DB CPU）
    - TOTAL WAITS: 统计周期内的等待次数
    - TIME(us): 统计周期内的等待总时间（微秒）
    - AVG(us): 单次等待平均耗时（微秒）
    - PCT: 时间占比 = event_time / (sum_all_non_idle_waits + DB_CPU)
    - WAIT_CLASS: 等待事件类别

两种展示模式（快捷键切换）:
    - RT (Real-Time): 实时模式，展示相邻两次采样的增量数据
    - C (Cumulative): 累计模式，展示实例启动以来的累计数据

数据来源:
    - v$system_event: 系统级等待事件统计（排除 Idle 类等待）
    - v$sys_time_model: DB CPU 时间
"""

import curses
from .monitor_base import Monitor

MONITOR_NAME = 'event'
MONITOR_HEIGHT = 7
MONITOR_LOG_LEVEL = 'WARNING'
MONITOR_CONFIG = "./monitor/event.cfg"

class EventMonitor(Monitor):
    def __init__(self):
        super().__init__(MONITOR_NAME, MONITOR_HEIGHT, MONITOR_LOG_LEVEL)
        self.last_total_time = 0
        self.last_cpu_time = 0
        self.last_event_result = None
        self.immediate = True
        self.curr_immediate = True

    def init(self, begin_x, begin_y, width):
        super().init(begin_x, begin_y, width)
        self.parse_config(MONITOR_CONFIG)

    def parse_config(self, config_file):
        if not config_file:
            return
        with open(self.base_path(config_file), 'r') as file:
            for line in file:
                line = line.strip()
                if ':' in line:
                    item, width = line.split(':', 1)
                    self.monitor_item.append(item)
                    self.monitor_width.append(int(width))

    def refresh(self):
        self.curr_immediate = self.immediate
        self.refresh_event()

    def refresh_event(self):
        tmp_monitor_value = []

        query = """
            SELECT
                (SELECT value FROM v$sys_time_model WHERE stat_name = 'DB CPU') AS CPU_TIME,
                e.EVENT,
                e.TOTAL_WAITS,
                e.TIME_WAITED_MICRO,
                ROUND(e.TIME_WAITED_MICRO / DECODE(e.TOTAL_WAITS, 0, 1, e.TOTAL_WAITS), 2) AS AVG_WAIT_MICRO,
                e.WAIT_CLASS
            FROM v$system_event e
            WHERE e.WAIT_CLASS != 'Idle'
              AND e.TOTAL_WAITS > 0
            ORDER BY e.TOTAL_WAITS DESC
            """
        sql_result = self.execute_query(query)
        if sql_result is None:
            self.logger.error("Exec query failed.")
            return

        row_value = sql_result[0]
        cur_cpu_time = row_value[0]
        cur_total_time = sum(list(zip(*sql_result))[3])
        if cur_total_time == 0 or cur_cpu_time == 0:
            self.logger.error("Invalid total_time: %d or cpu_time: %d.", cur_total_time, cur_cpu_time)
            return

        total_time_diff = 0
        if not self.curr_immediate:
            cpu_time_diff = cur_cpu_time
            total_time_diff = cur_total_time + cur_cpu_time
        else:
            cpu_time_diff = cur_cpu_time - self.last_cpu_time
            total_time_diff = cur_total_time - self.last_total_time + cpu_time_diff

        cpu_time_pct = round(cpu_time_diff / total_time_diff, 4) if total_time_diff > 0 else 0
        monitor_value_per_line = ["DB CPU", "", cpu_time_diff, "", cpu_time_pct, ""]
        tmp_monitor_value.append(monitor_value_per_line)

        self.last_total_time = cur_total_time
        self.last_cpu_time = cur_cpu_time

        if not self.curr_immediate:
            for row_id, row_value in enumerate(sql_result):
                monitor_value_per_line = []
                for col_id, col_value in enumerate(row_value):
                    if col_id == 0:
                        continue
                    elif col_id == 4:
                        monitor_value_per_line.append(col_value)
                        monitor_value_per_line.append(round(monitor_value_per_line[2] / total_time_diff, 2) if total_time_diff > 0 else 0)
                    else:
                        monitor_value_per_line.append(col_value)
                tmp_monitor_value.append(monitor_value_per_line)
        else:
            last_event_result_dict = dict()
            if self.last_event_result is not None:
                for row_value in self.last_event_result:
                    event_name = row_value[1]
                    last_event_result_dict[event_name] = row_value

            for row_id, row_value in enumerate(sql_result):
                monitor_value_per_line = []
                for col_id, col_value in enumerate(row_value):
                    if col_id == 0:
                        continue
                    elif col_id == 1 or col_id == 5:
                        monitor_value_per_line.append(col_value)
                    elif col_id == 2 or col_id == 3:
                        event_name = monitor_value_per_line[0]
                        last_event_result_row = last_event_result_dict.get(event_name)
                        if last_event_result_row is None:
                            last_event_result_value = 0
                        else:
                            last_event_result_value = last_event_result_row[col_id]
                        monitor_value_per_line.append(col_value - last_event_result_value)
                    elif col_id == 4:
                        event_wait = monitor_value_per_line[1]
                        event_time = monitor_value_per_line[2]
                        if event_time <= 0 or event_wait <= 0:
                            monitor_value_per_line.append(0)
                            monitor_value_per_line.append(0)
                        else:
                            monitor_value_per_line.append(round(event_time / event_wait, 2))
                            monitor_value_per_line.append(round(event_time / total_time_diff, 4) if total_time_diff > 0 else 0)
                    else:
                        self.logger.error("Invalid case.")
                tmp_monitor_value.append(monitor_value_per_line)

        tmp_monitor_value.sort(key=lambda x: x[4], reverse=True)

        with self.lock:
            self.monitor_value = tmp_monitor_value

        self.last_event_result = sql_result

    def print(self, stdscr):
        super().clear_screen(stdscr)

        start_y = 0
        start_x = self.begin_x
        white_bg = None
        if stdscr is not None:
            white_bg = curses.color_pair(2)

        for i, item in enumerate(self.monitor_item):
            if i == 0:
                if self.curr_immediate:
                    self.printer.addstr(start_y, start_x, f"{item}(RT)", white_bg)
                else:
                    self.printer.addstr(start_y, start_x, f"{item}(C)", white_bg)
            else:
                self.printer.addstr(start_y, start_x, f"{item}", white_bg)
            start_x += self.monitor_width[i]

        start_y = 1
        start_x = self.begin_x
        for i, monitor_value_per_line in enumerate(self.monitor_value):
            if i == (MONITOR_HEIGHT - 1):
                break
            for j, value in enumerate(monitor_value_per_line):
                data = f"{value}"
                if j == 4:
                    data = f"{value:.2%}"
                self.printer.addstr(start_y, start_x, data)
                start_x += self.monitor_width[j]
            start_y += 1
            start_x = self.begin_x

        super().print(stdscr)
