# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from .monitor_base import Monitor
from common.config import Config
from common import util
import curses
import os
import time
import subprocess
import threading

MONITOR_NAME = 'os'
MONITOR_HEIGHT = 2
MONITOR_LOG_LEVEL = 'INFO'
MONITOR_CONFIG = "./monitor/operating_system.cfg"

class OSMonitor(Monitor):
    def __init__(self):
        super().__init__(MONITOR_NAME, MONITOR_HEIGHT, MONITOR_LOG_LEVEL)
        self.physical_devices = None
        self.os_cmd = []
        self.os_value = []
        self.value_type = []
        self.multi_disk = None
        self.prev_diskstats = [0] * 6
        self.cur_diskstats = [0] * 6
        self.last_refresh_disk_time = None
        self.last_cpu_info = [0] * 7
        self.cur_cpu_info = [0] * 7
        self.last_refresh_cpu_time = None
        self.log_width = []
        self.log_item = []
        self.aqu_sz = 0

    def init(self, begin_x, begin_y, width):
        super().init(begin_x, begin_y, width)
        self.parse_config(MONITOR_CONFIG)

        self.init_physical_devices()
        if self.physical_devices is None or len(self.physical_devices) == 0:
            raise RuntimeError("Get Oracle physical device failed")

        device_names = [os.path.basename(dev) for dev in self.physical_devices]
        self.multi_disk = False
        if len(self.physical_devices) > 1:
            self.multi_disk = True
        grep_pattern = " || ".join([f'$3 == "{name}"' for name in device_names])
        self.logger.info("Oracle physical device: %s", grep_pattern)

        self.os_cmd = [0] * 4
        self.os_cmd[0] = f"awk '{grep_pattern}' /proc/diskstats"
        self.os_cmd[1] = "head -n 1 /proc/stat"
        self.os_cmd[2] = "awk '/MemTotal/ {total=$2} /MemAvailable/ {available=$2} END {used=total-available; print (used/total)*100}' /proc/meminfo"
        self.os_cmd[3] = "uptime | awk -F'load average: ' '{print $2}' | awk '{print $1}' | sed 's/,//'"
        self.os_value = [0] * 4
        self.monitor_value = [0] * len(self.monitor_item)
        self.last_refresh_disk_time = 0
        self.last_refresh_cpu_time = 0

        # start io refresh thread
        io_thread = threading.Thread(target=self.io_refresher, args=(Config.get("main.interval"),))
        io_thread.daemon = True
        io_thread.start()

    def get_physical_device(self, device_path: str):
        dev_name = os.path.basename(device_path)
        lsblk_output = self.execute_os_command("lsblk -nl -o NAME,TYPE,PKNAME")
        if lsblk_output is None:
            raise RuntimeError("lsblk command return none")

        child_to_parents = {}
        name_to_type = {}

        for line in lsblk_output.splitlines():
            parts = line.strip().split()
            if len(parts) == 2:
                name, typ = parts
                pkname = None
            elif len(parts) == 3:
                name, typ, pkname = parts
            else:
                continue

            name_to_type[name] = typ
            if pkname:
                child_to_parents.setdefault(name, []).append(pkname)

        queue = [dev_name]
        visited = set()
        physical_devices = set()

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)

            parents = child_to_parents.get(current, [])
            if not parents:
                if name_to_type.get(current) == "disk":
                    physical_devices.add(current)
            else:
                for parent in parents:
                    if name_to_type.get(parent) == "disk":
                        physical_devices.add(parent)
                    else:
                        queue.append(parent)

        return sorted(physical_devices)

    def init_physical_devices(self):
        # For Oracle, get the datafile directory via SQL query
        query = "SELECT name FROM v$datafile WHERE ROWNUM = 1"
        result = self.execute_query(query)
        if result is None or len(result) == 0:
            # Fallback: try to find Oracle process data directory
            get_data_path_cmd = "ps -ux | grep ora_pmon | grep -v grep | awk '{print $NF}' | head -1"
            data_path = self.execute_os_command(get_data_path_cmd)
            if data_path is None or len(data_path) == 0:
                raise RuntimeError(f"Get Oracle data path failed")
            # Use Oracle base directory
            data_path = os.path.dirname(data_path)
        else:
            data_path = os.path.dirname(result[0][0])

        if not os.path.exists(data_path):
            raise RuntimeError(f"Oracle data path does not exist: {data_path}")

        df_command = f"df -P {data_path} | awk 'NR==2 {{print $1}}'"
        device = self.execute_os_command(df_command)
        if device is None:
            raise RuntimeError("Get Oracle device failed, cmd: %s", df_command)

        self.physical_devices = self.get_physical_device(device)

    def get_io_stat(self, monitor_item, interval):
        if interval == 0:
            return 0

        if monitor_item == 'r/s':
            return (self.cur_diskstats[0] - self.prev_diskstats[0]) / interval
        elif monitor_item == 'w/s':
            return (self.cur_diskstats[2] - self.prev_diskstats[2]) / interval
        elif monitor_item == 'rkB/s':
            return (self.cur_diskstats[1] - self.prev_diskstats[1]) * 512 / interval / 1024
        elif monitor_item == 'wkB/s':
            return (self.cur_diskstats[3] - self.prev_diskstats[3]) * 512 / interval / 1024
        elif monitor_item == 'r_asize(kB)':
            if self.cur_diskstats[0] - self.prev_diskstats[0] > 0:
                return (self.cur_diskstats[1] - self.prev_diskstats[1]) / (
                            self.cur_diskstats[0] - self.prev_diskstats[0]) * 512 / interval / 1024
            else:
                return 0
        elif monitor_item == 'w_asize(kB)':
            if self.cur_diskstats[2] - self.prev_diskstats[2] > 0:
                return (self.cur_diskstats[3] - self.prev_diskstats[3]) / (
                            self.cur_diskstats[2] - self.prev_diskstats[2]) * 512 / interval / 1024
            else:
                return 0
        elif monitor_item == 'r_await':
            read_diff = self.cur_diskstats[0] - self.prev_diskstats[0]
            time_diff = self.cur_diskstats[4] - self.prev_diskstats[4]
            if read_diff > 0:
                return time_diff / read_diff
            else:
                return 0
        elif monitor_item == 'w_await':
            write_diff = self.cur_diskstats[2] - self.prev_diskstats[2]
            time_diff = self.cur_diskstats[5] - self.prev_diskstats[5]
            if write_diff > 0:
                return time_diff / write_diff
            else:
                return 0
        elif monitor_item == 'aqu-sz':
            return self.aqu_sz
        else:
            self.logger.error("item not defined.")
            return 0

    def parse_config(self, config_file):
        if not config_file:
            return

        with open(self.base_path(config_file), 'r') as file:
            for line in file:
                line = line.strip()
                if ':' in line:
                    item, width, vtype, log_item, log_width = line.split(':', 4)
                    self.monitor_item.append(item)
                    self.monitor_width.append(int(width))
                    self.value_type.append(vtype)
                    self.log_item.append(log_item)
                    self.log_width.append(int(log_width))

    def refresh(self):
        cur_timestamp = time.time()

        for i in range(len(self.os_cmd)):
            self.os_value[i] = self.execute_os_command(self.os_cmd[i])

        # build self.cur_diskstats
        if self.os_value[0] is None:
            self.cur_diskstats = [0] * 6
        else:
            if self.multi_disk:
                stats = [0] * 6
                for line in self.os_value[0].splitlines():
                    fields = line.split()
                    stats[0] += int(fields[3])
                    stats[1] += int(fields[5])
                    stats[2] += int(fields[7])
                    stats[3] += int(fields[9])
                    stats[4] += int(fields[6])
                    stats[5] += int(fields[10])
                for i in range(6):
                    self.cur_diskstats[i] = stats[i]
            else:
                fields = self.os_value[0].split()
                self.cur_diskstats[0] = int(fields[3])    # reads completed successfully
                self.cur_diskstats[1] = int(fields[5])    # sectors read
                self.cur_diskstats[2] = int(fields[7])    # writes completed
                self.cur_diskstats[3] = int(fields[9])    # sectors written
                self.cur_diskstats[4] = int(fields[6])    # time spent reading (ms)
                self.cur_diskstats[5] = int(fields[10])   # time spent writing (ms)

        # build self.cur_cpu_info
        if self.os_value[1] is None:
            self.cur_cpu_info = [0] * 7
        else:
            cpu_info = self.os_value[1].split()
            for i in range(len(self.cur_cpu_info)):
                self.cur_cpu_info[i] = int(cpu_info[i + 1])

        for i in range(len(self.monitor_item)):
            if self.value_type[i] == "IO":
                if self.os_value[0] is None or self.last_refresh_disk_time == 0:
                    self.monitor_value[i] = 0
                else:
                    interval = cur_timestamp - self.last_refresh_disk_time
                    self.monitor_value[i] = round(self.get_io_stat(self.monitor_item[i], interval), 2)
                continue
            if self.value_type[i] == "CPU":
                if self.os_value[1] is None or self.last_refresh_cpu_time == 0:
                    self.monitor_value[i] = 0
                else:
                    total = 0
                    busy = 0
                    for j in range(len(self.cur_cpu_info)):
                        total += (self.cur_cpu_info[j] - self.last_cpu_info[j])
                        if j != 3:
                            busy += (self.cur_cpu_info[j] - self.last_cpu_info[j])
                    self.monitor_value[i] = round((busy / total) * 100, 2)
                # save current cpu usage
                util.update_cpu_usage(self.monitor_value[i])
                continue
            if self.value_type[i] == "MEM":
                if self.os_value[2] is None:
                    self.monitor_value[i] = 0
                else:
                    self.monitor_value[i] = round(float(self.os_value[2]), 2)
                continue
            if self.value_type[i] == "LOAD":
                if self.os_value[3] is None:
                    self.monitor_value[i] = 0
                else:
                    self.monitor_value[i] = round(float(self.os_value[3]), 2)
                continue

        # only save valid data
        if self.os_value[0] is not None:
            self.last_refresh_disk_time = cur_timestamp
            self.prev_diskstats = self.cur_diskstats.copy()

        if self.os_value[1] is not None:
            self.last_refresh_cpu_time = cur_timestamp
            self.last_cpu_info = self.cur_cpu_info.copy()

        # alarm logic
        self.check_and_report_alarm()

    def io_refresher(self, interval):
        io_command = f"iostat -x {interval}"
        process = subprocess.Popen(
            io_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        sum_aqu_sz = 0
        iostat_data_start = False
        header = []
        while True:
            line = process.stdout.readline().strip()
            if line is None:
                continue

            if iostat_data_start:
                if len(line) == 0:
                    iostat_data_start = False
                    self.aqu_sz = sum_aqu_sz
                    sum_aqu_sz = 0
                else:
                    parts = line.split()
                    if len(parts) == len(header):
                        device = parts[0]
                        if device in self.physical_devices:
                            stats = dict(zip(header, parts))
                            aqu_sz = stats.get("aqu-sz", None)
                            if aqu_sz is not None:
                                sum_aqu_sz += float(aqu_sz)
                continue

            if line.startswith("Device"):
                iostat_data_start = True
                header = line.split()

    def print(self, stdscr):
        # send to logger thread
        if self.message_queue:
            self.message_queue.put(("os", zip(self.log_item, self.monitor_value, self.log_width)))

        # first clear the screen
        super().clear_screen(stdscr)

        # print header
        start_x = 0
        start_y = 0
        white_bg = None
        if stdscr is not None:
            white_bg = curses.color_pair(2)
        for i, item in enumerate(self.monitor_item):
            self.printer.addstr(start_y, start_x, f"{item}", white_bg)
            start_x += self.monitor_width[i]

        # print value
        start_y = 1
        start_x = self.begin_x
        for i, value in enumerate(self.monitor_value):
            data = f"{value}"
            self.printer.addstr(start_y, start_x, data)
            start_x += self.monitor_width[i]

        # print to screen
        super().print(stdscr)
