# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from .monitor_base import Monitor
from common.config import Config

import curses
import re
import time
import threading
import subprocess

MONITOR_NAME = 'instance'
MONITOR_HEIGHT = 2
MONITOR_LOG_LEVEL = 'WARNING'
MONITOR_CONFIG = "./monitor/instance.cfg"
SESSION_ITEMS = ["SN", "AN", "ASC", "ASI", "IDL"]

GET_MAX_SESSIONS_SQL = "SELECT value FROM v$parameter WHERE name='sessions'"
GET_MAX_PROCESSES_SQL = "SELECT value FROM v$parameter WHERE name='processes'"

def extract_number(string):
    number_and_measurement = re.findall(r"\d+\.\d+|\d+", string)
    number_len = len(number_and_measurement[0])
    measurement = string[number_len:]
    number_and_measurement.append(measurement)
    return number_and_measurement

class InsMonitor(Monitor):
    def __init__(self):
        super().__init__(MONITOR_NAME, MONITOR_HEIGHT, MONITOR_LOG_LEVEL)
        self.tmp_value = []
        self.session_result = None
        self.log_width = []
        self.log_item = []
        self.io_record = None
        self.interval = 0
        self.max_sessions = 0
        self.max_processes = 0

    def io_refresher(self, interval):
        rd_col = 0
        wr_col = 0
        # Use pidstat on Oracle background writer process
        io_command = "pidstat -d -p $(pgrep -f 'ora_dbw' | head -1) %d" % interval
        process = subprocess.Popen(
            io_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        while True:
            return_code = subprocess.Popen.poll(process)
            if return_code is not None:
                self.logger.warning("io refresh thread exit, return code: %d" % return_code)
                break

            line = process.stdout.readline().strip()
            if len(line) == 0:
                continue

            splited_line = line.split()
            if len(splited_line) <= 7:
                continue

            if splited_line[len(splited_line) - 1] == 'Command':
                for col_idx, col_name in enumerate(splited_line):
                    if col_name == 'kB_rd/s':
                        rd_col = col_idx
                    elif col_name == 'kB_wr/s':
                        wr_col = col_idx
                    else:
                        continue
                continue

            self.io_record = round((float(splited_line[rd_col]) + float(splited_line[wr_col])) / 1024, 2)
            time.sleep(interval)

    def init(self, begin_x, begin_y, width):
        super().init(begin_x, begin_y, width)
        self.parse_config(MONITOR_CONFIG)

        self.tmp_value = [0] * len(self.monitor_item)
        self.monitor_value = [0] * len(self.monitor_item)

        show_result = self.execute_query(GET_MAX_SESSIONS_SQL)
        if show_result is None:
            raise RuntimeError('Failed to get max sessions')
        self.max_sessions = int(show_result[0][0])

        proc_result = self.execute_query(GET_MAX_PROCESSES_SQL)
        if proc_result is not None:
            self.max_processes = int(proc_result[0][0])

        # start io refresh thread
        io_thread = threading.Thread(target=self.io_refresher, args=(Config.get("main.interval"),))
        io_thread.daemon = True
        io_thread.start()

    def parse_config(self, config_file):
        if not config_file:
            return
        with open(self.base_path(config_file), 'r') as file:
            for line in file:
                line = line.strip()
                if ':' in line:
                    item, width, method, log_item, log_width = line.split(':', 4)
                    self.monitor_item.append(item)
                    self.monitor_width.append(int(width))
                    self.monitor_method.append(method)
                    self.log_item.append(log_item)
                    if log_width != '':
                        self.log_width.append(int(log_width))
                    else:
                        self.log_width.append(0)

    def refresh(self):
        for i in range(len(self.monitor_item)):
            process_data = ""
            if self.monitor_method[i] != "":
                result = self.execute_query(self.monitor_method[i])
                if result is None:
                    self.logger.error(f"Exec query for monitor_item {self.monitor_item[i]} returned None.")
                    return
                if self.monitor_item[i] == "SN":
                    self.session_result = result[0]
                elif self.monitor_item[i] == "PROCESSES":
                    process_data = result[0][0]
                else:
                    for row in result:
                        process_data = row[0]

            if self.monitor_item[i] == "time":
                if self.tmp_value[i] != 0:
                    time_diff = process_data - self.tmp_value[i]
                    self.interval = time_diff.total_seconds()
                self.tmp_value[i] = process_data
            elif self.monitor_item[i] == "P95(ms)":
                # Oracle doesn't have a built-in P95 view; compute or show N/A
                self.monitor_value[i] = "N/A"
            elif self.monitor_item[i] == "REDO(kB/s)":
                if self.tmp_value[i] != 0 and process_data >= self.tmp_value[i] and self.interval > 0:
                    self.monitor_value[i] = round(float(process_data - self.tmp_value[i])/1024/self.interval, 2)
                else:
                    self.monitor_value[i] = 0
                self.tmp_value[i] = process_data
            elif self.monitor_item[i] == "TPS" or self.monitor_item[i] == "QPS":
                if self.tmp_value[i] != 0 and process_data >= self.tmp_value[i] and self.interval > 0:
                    self.monitor_value[i] = round(float(process_data - self.tmp_value[i])/self.interval, 2)
                else:
                    self.monitor_value[i] = 0
                self.tmp_value[i] = process_data
            elif self.monitor_item[i] == "MBPS":
                self.monitor_value[i] = self.io_record
            elif self.monitor_item[i] == "CONNECTION(c/m)":
                session_num = self.monitor_value[1]  # "SN"
                if self.max_sessions > 0:
                    connections_pct = round(float(session_num)/self.max_sessions * 100, 2)
                    self.monitor_value[i] = f'{connections_pct}%({session_num}/{self.max_sessions})'
                else:
                    self.monitor_value[i] = f'N/A({session_num})'
            elif self.monitor_item[i] == "PROCESSES":
                if self.max_processes > 0:
                    proc_pct = round(float(process_data)/self.max_processes * 100, 2)
                    self.monitor_value[i] = f'{proc_pct}%({process_data}/{self.max_processes})'
                else:
                    self.monitor_value[i] = f'{process_data}'
            elif self.monitor_item[i] in SESSION_ITEMS:
                self.monitor_value[i] = self.session_result[SESSION_ITEMS.index(self.monitor_item[i])]

        self.check_and_report_alarm()

    def print(self, stdscr):
        if self.message_queue:
            self.message_queue.put(("ins", zip(self.log_item[1:], self.monitor_value[1:], self.log_width[1:])))

        super().clear_screen(stdscr)

        start_x = 0
        start_y = 0
        white_bg = None
        if stdscr is not None:
            white_bg = curses.color_pair(2)
        for i, item in enumerate(self.monitor_item):
            self.printer.addstr(start_y, start_x, f"{item}", white_bg)
            if len(item) < self.monitor_width[i]:
                self.printer.addstr(start_y, start_x + len(item), ' ' * (self.monitor_width[i] - len(item)), white_bg)
            start_x += self.monitor_width[i]

        start_line = 1
        begin_x = self.begin_x
        for i, value in enumerate(self.monitor_value):
            data = f"{value}"
            self.printer.addstr(start_line, begin_x, data)
            if len(data) < self.monitor_width[i]:
                self.printer.addstr(start_line, begin_x + len(data), ' ' * (self.monitor_width[i] - len(data)))
            begin_x += self.monitor_width[i]

        super().print(stdscr)
