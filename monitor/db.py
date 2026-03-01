# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from .monitor_base import Monitor
from common import util

MONITOR_NAME = 'db'
MONITOR_HEIGHT = 1
MONITOR_LOG_LEVEL = 'WARNING'
MONITOR_CONFIG = "./monitor/db.cfg"
DB_BUSY_MONITOR_LIST = ["db%", "WTR%"]

class DBMonitor(Monitor):
    def __init__(self):
        super().__init__(MONITOR_NAME, MONITOR_HEIGHT, MONITOR_LOG_LEVEL)
        self.tmp_value = []
        self.os_method = []
        self.busy_result = None
        self.role_value = None
        self.counter = 0
        self.version = None
        self.user = None
        self.pga = 0
        self.db_info = None
        self.log_width = []
        self.log_item = []

    def init(self, begin_x, begin_y, width):
        super().init(begin_x, begin_y, width)
        self.parse_config(MONITOR_CONFIG)
        self.tmp_value = [0] * len(self.monitor_item)
        self.monitor_value = [0] * len(self.monitor_item)

    def parse_config(self, config_file):
        if not config_file:
            return

        with open(self.base_path(config_file), 'r') as file:
            for line in file:
                line = line.strip()
                if '|' in line:
                    item, width, method, os_method, log_item, log_width = line.split('|', 5)
                    self.monitor_item.append(item)
                    self.monitor_width.append(int(width))
                    self.monitor_method.append(method)
                    self.os_method.append(os_method)
                    self.log_item.append(log_item)
                    if log_width != '':
                        self.log_width.append(int(log_width))
                    else:
                        self.log_width.append(0)

    def refresh(self):
        self.counter += 1
        tmp_monitor_value = [0] * len(self.monitor_item)
        for i in range(len(self.monitor_item)):
            if i == 0:  # version
                if self.version is not None:
                    tmp_monitor_value[i] = self.version
                    continue
            elif i == 1:  # user
                if self.user is not None:
                    tmp_monitor_value[i] = self.user
                    continue
            elif self.monitor_item[i] == "ROLE":
                if self.role_value is not None and len(self.role_value) > 0 and self.counter <= 1000:
                    tmp_monitor_value[i] = self.role_value
                    continue
            elif self.monitor_item[i] == "MB PGA":
                if not util.should_refresh_memory("pga"):
                    tmp_monitor_value[i] = self.pga
                    continue

            process_data = ""
            # Phase 1: Execute SQL
            if self.monitor_method[i] != "":
                result = self.execute_query(self.monitor_method[i])
                if result is None:
                    continue
                if self.monitor_item[i] == "db%":
                    self.busy_result = result[0]
                    self.logger.debug("busy_result %s", self.busy_result)
                else:
                    for row in result:
                        process_data = row[0]

            # Phase 2: Execute shell command
            if process_data != "":
                if self.os_method[i] != "":
                    tmp_monitor_value[i] = self.execute_os_command(f"echo \"{process_data}\" | {self.os_method[i]}")
                else:
                    tmp_monitor_value[i] = process_data
            else:
                if self.os_method[i] != "":
                    tmp_monitor_value[i] = self.execute_os_command(self.os_method[i])

            # Phase 3: Handle data
            if self.monitor_item[i] == "MB SGA":
                if tmp_monitor_value[i] is not None and tmp_monitor_value[i] != 0:
                    tmp_monitor_value[i] = "{:.6g}".format(round(float(tmp_monitor_value[i]), 2))
            elif self.monitor_item[i] == "MB PGA":
                if tmp_monitor_value[i] is not None and tmp_monitor_value[i] != 0:
                    tmp_monitor_value[i] = "{:.6g}".format(round(float(tmp_monitor_value[i]), 2))
                self.pga = tmp_monitor_value[i]
            elif self.monitor_item[i] == "ROLE":
                self.role_value = tmp_monitor_value[i]
                if self.db_info:
                    if self.is_primary(self.role_value):
                        self.db_info.role = "primary"
                    else:
                        self.db_info.role = "standby"
                self.counter = 0
            elif i == 0:  # version
                self.version = tmp_monitor_value[i]
                if self.db_info:
                    self.db_info.current_version = tmp_monitor_value[i]
            elif i == 1:  # user
                self.user = tmp_monitor_value[i]
                if self.db_info:
                    self.db_info.username = tmp_monitor_value[i]
            elif self.monitor_item[i] in DB_BUSY_MONITOR_LIST:
                if self.tmp_value[i] != 0:
                    cpu_time = self.busy_result[0] - self.tmp_value[i][0]
                    db_time = self.busy_result[1] - self.tmp_value[i][1]
                    # Oracle v$sys_time_model returns microseconds
                    timestamp_diff = (self.busy_result[2] - self.tmp_value[i][2]).total_seconds() * 1000000
                    if self.monitor_item[i] == "db%" and cpu_time > 0 and tmp_monitor_value[i] is not None:
                        nproc = int(tmp_monitor_value[i])
                        cpu_time_pct = round((cpu_time / (timestamp_diff * nproc)) * 100, 2)
                        tmp_monitor_value[i] = f'{cpu_time_pct}'
                    elif self.monitor_item[i] == "WTR%" and db_time > 0:
                        tmp_monitor_value[i] = round((db_time - cpu_time)/db_time * 100, 2)
                    else:
                        tmp_monitor_value[i] = 0
                else:
                    tmp_monitor_value[i] = 0
                self.tmp_value[i] = self.busy_result

        with self.lock:
            self.monitor_value = tmp_monitor_value

    def is_primary(self, role_value):
        if role_value is None:
            return False
        return 'PRIMARY' in str(role_value).upper()

    def set_db_info_container(self, db_info):
        self.db_info = db_info

    def print(self, stdscr):
        if self.message_queue:
            self.message_queue.put(("db", zip(self.log_item[4:], self.monitor_value[4:], self.log_width[4:])))

        super().clear_screen(stdscr, False)

        start_y = 0
        start_x = self.begin_x
        for i, value in enumerate(self.monitor_value):
            data = f"{value} {self.monitor_item[i]}"
            self.printer.addstr(start_y, start_x, data)
            start_x += self.monitor_width[i]

        super().print(stdscr)
