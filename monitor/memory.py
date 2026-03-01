# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
内存监控模块 (Memory Monitor)

独立视图（快捷键 'm' 切换），深度分析 Oracle SGA 和 PGA 内存使用情况。

四个监控面板:
    Panel 0 - SGA/PGA 概览:
        SGA 最大值、已用百分比、空闲量；PGA 分配量、已用量、可释放量

    Panel 1 - TOP 5 SGA 组件:
        按 pool 聚合的 SGA 组件大小（shared pool, buffer cache, large pool 等）

    Panel 2 - TOP 10 会话内存 (PGA):
        按 PGA 分配量降序排列的 Top 10 用户会话（SID, USERNAME, PROGRAM, PGA 详情）

    Panel 3 - TOP 10 进程内存 (PGA):
        按 PGA 分配量降序排列的 Top 10 Oracle 进程（PID, PROGRAM, PGA 详情）

运行机制:
    - 独立刷新线程，刷新间隔由 main.mem_interval 控制
    - CPU 高负载时自动跳过 Panel 2/3 的刷新（降低采集开销）
    - 支持与应急模块联动：SGA 满/PGA 异常时高亮显示

数据来源:
    - v$sgainfo: SGA 总览
    - v$sgastat: SGA 组件明细
    - v$pgastat: PGA 聚合统计
    - v$session + v$process: 会话和进程级 PGA 明细
"""

import curses
from datetime import datetime
import threading
import traceback
import time

from .monitor_base import Monitor
from common.config import Config
from common import util

MONITOR_NAME = 'memory'
MONITOR_HEIGHT = 43
MONITOR_LOG_LEVEL = 'WARNING'
MONITOR_CONFIG = "./monitor/memory.cfg"

# emergency mode
EMER_NULL = 0
EMER_SGA_MEMORY_FULL = 1
EMER_SESSION_PGA_MEMORY_FULL = 2


class MemoryMonitor(Monitor):
    def __init__(self):
        super().__init__(MONITOR_NAME, MONITOR_HEIGHT, MONITOR_LOG_LEVEL)
        self.stdscr = None
        self.paused = False
        self.refresh_thread = None
        self.monitor_panels = None
        self.print_to_screen = False
        self.memory_full_type = EMER_NULL

    def init(self, begin_x, begin_y, width):
        super().init(begin_x, begin_y, width)
        self.parse_config(MONITOR_CONFIG)

        # each value of monitor_panels is a dict
        # each dict has 4 keys: "title" "header" "width" "value"
        self.monitor_panels = [
            {"title": None, "header": [], "width": [], "value": []},
            {"title": "TOP 5 SGA COMPONENTS", "header": [], "width": [], "value": []},
            {"title": "TOP 10 SESSION MEMORY (PGA)", "header": [], "width": [], "value": []},
            {"title": "TOP 10 PROCESS MEMORY (PGA)", "header": [], "width": [], "value": []}
        ]

        # start refresh thread
        self.refresh_thread = threading.Thread(target=self.wrapper)
        self.refresh_thread.start()

    def stop(self):
        self.logger.warning("The memory monitor refresh thread is starting to exit.")
        self.paused = True
        self.refresh_thread.join()
        self.logger.warning("The memory monitor refresh thread has exited.")

    def get_monitor_panels(self):
        return self.monitor_panels

    def set_memory_full_type(self, memory_full_type):
        self.memory_full_type = memory_full_type

    def wrapper(self):
        while True and not self.paused:
            try:
                # record start time
                start_time = time.perf_counter()

                self.refresh()
                self.print(self.stdscr)

                if self.paused:
                    return

                # calculate sleep time
                elapsed_time = time.perf_counter() - start_time
                sleep_time = max(0, Config.get("main.mem_interval") - elapsed_time)
                if sleep_time > 0:
                    time.sleep(sleep_time)
            except Exception:
                self.logger.error("Memory Monitor Exception Traceback:\n%s",
                                  traceback.format_exc())

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

    def refresh_summary_info(self):
        """
            Panel0 - SGA and PGA summary
        """
        panel = self.monitor_panels[0]
        panel["header"] = self.monitor_item
        panel["width"] = self.monitor_width
        self.monitor_value = []

        curr_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # default all zero
        monitor_value_per_line = [curr_time, 0, 0, 0, 0, 0, 0, 0, 0]

        # Query SGA info
        sga_query = """SELECT name, bytes FROM v$sgainfo WHERE name IN ('Maximum SGA Size', 'Free SGA Memory Available')"""
        sga_result = self.execute_query(sga_query)

        # Query PGA info
        pga_query = """SELECT name, value FROM v$pgastat WHERE name IN ('aggregate PGA target parameter', 'total PGA allocated', 'total freeable PGA memory')"""
        pga_result = self.execute_query(pga_query)

        if sga_result is None and pga_result is None:
            self.logger.error("Exec SGA/PGA query failed.")
            self.monitor_value.append(monitor_value_per_line)
            panel["value"] = self.monitor_value
            return

        sga_max = 0
        sga_free = 0
        pga_target = 0
        pga_allocated = 0
        pga_freeable = 0

        if sga_result is not None:
            for row in sga_result:
                if row[0] == 'Maximum SGA Size':
                    sga_max = round(row[1] / 1024 / 1024, 2)
                elif row[0] == 'Free SGA Memory Available':
                    sga_free = round(row[1] / 1024 / 1024, 2)

        if pga_result is not None:
            for row in pga_result:
                if row[0] == 'aggregate PGA target parameter':
                    pga_target = round(row[1] / 1024 / 1024, 2)
                elif row[0] == 'total PGA allocated':
                    pga_allocated = round(row[1] / 1024 / 1024, 2)
                elif row[0] == 'total freeable PGA memory':
                    pga_freeable = round(row[1] / 1024 / 1024, 2)

        sga_used_pct = round((sga_max - sga_free) / sga_max * 100, 2) if sga_max > 0 else 0
        sga_free_pct = round(sga_free / sga_max * 100, 2) if sga_max > 0 else 0
        pga_used = round(pga_allocated - pga_freeable, 2)
        pga_free_pct = round(pga_freeable / pga_allocated * 100, 2) if pga_allocated > 0 else 0

        monitor_value_per_line = [curr_time,
                                  sga_max, sga_used_pct,
                                  sga_free, sga_free_pct,
                                  pga_allocated, pga_used, pga_freeable, pga_free_pct]
        self.monitor_value.append(monitor_value_per_line)
        panel["value"] = self.monitor_value

    def calculate_delta(self, reserved_width, data_array):
        col_num = 0
        context_name_length = 0
        for idx, row in enumerate(data_array):
            if idx >= 5:
                break
            col_num += 1
            context_name = row[0]
            context_name_length += len(context_name)
        return int((self.width - 1 - reserved_width - context_name_length) / (col_num - 1)) if col_num > 1 else 0

    @staticmethod
    def panel_add_column(panel, col_width, col_values):
        header = panel["header"]
        width = panel["width"]
        value = panel["value"]

        width.append(col_width)
        for idx, col_value in enumerate(col_values):
            if idx == 0:
                header.append(col_value)
            else:
                value[idx - 1].append(col_value)

    def refresh_sga_info(self):
        """
            Panel1 - SGA component details
        """
        panel = self.monitor_panels[1]
        panel["header"] = []
        panel["width"] = []
        panel["value"] = [[] for _ in range(2)]  # only 2 lines: TOTAL and FREE

        query = """
            SELECT pool, name,
                   ROUND(bytes/1024/1024, 2) AS total_mb
            FROM v$sgastat
            WHERE bytes > 0
            ORDER BY bytes DESC"""
        sga_detail = self.execute_query(query)
        if sga_detail is None:
            self.logger.error("Exec SGA detail query failed.")
            self.panel_add_column(panel, 10, ["", "TOTAL", ""])
            self.panel_add_column(panel, 10, ["SUM", 0, 0])
            return

        # Aggregate by pool
        pool_dict = {}
        for row in sga_detail:
            pool = row[0] if row[0] else 'fixed'
            if pool not in pool_dict:
                pool_dict[pool] = 0
            pool_dict[pool] += row[2]

        # Sort by size desc
        sorted_pools = sorted(pool_dict.items(), key=lambda x: x[1], reverse=True)

        # calculate width delta
        delta = self.calculate_delta(10 + 10, [(p[0], p[1]) for p in sorted_pools])

        # add first column
        self.panel_add_column(panel, 10, ["", "TOTAL(MB)", ""])

        total_sum = sum(v for _, v in sorted_pools)
        self.panel_add_column(panel, 10, ["SUM", round(total_sum, 2), ""])

        for idx, (pool_name, total_size) in enumerate(sorted_pools):
            if idx >= 5:
                break
            delta_val = 0 if idx == 4 else delta
            self.panel_add_column(panel, len(pool_name) + delta_val,
                                  [pool_name, round(total_size, 2), ""])

    @staticmethod
    def panel_append_column(panel, row_id, col_width, col_value):
        header = panel["header"]
        width = panel["width"]
        value = panel["value"]

        if row_id == 0:
            header.append(col_value)
            width.append(col_width)
        else:
            value[row_id - 1].append(col_value)

    def refresh_session_info(self):
        """
            Panel2 - Top 10 sessions by PGA usage
        """
        panel = self.monitor_panels[2]
        panel["header"] = []
        panel["width"] = []
        panel["value"] = []

        query = """
            SELECT s.SID, s.USERNAME, s.PROGRAM,
                   ROUND(p.PGA_ALLOC_MEM/1024/1024, 2) AS pga_mb,
                   ROUND(p.PGA_USED_MEM/1024/1024, 2) AS pga_used_mb,
                   ROUND(p.PGA_FREEABLE_MEM/1024/1024, 2) AS pga_free_mb
            FROM v$session s
            JOIN v$process p ON s.PADDR = p.ADDR
            WHERE s.TYPE = 'USER'
            ORDER BY p.PGA_ALLOC_MEM DESC
            FETCH FIRST 10 ROWS ONLY
        """
        session_pga_detail = self.execute_query(query)
        if session_pga_detail is None:
            self.logger.error("Exec session PGA query failed.")
            return

        value_lines = min(len(session_pga_detail), 10)
        panel["value"] = [[] for _ in range(value_lines)]

        # build header
        self.panel_append_column(panel, 0, 10, "SID")
        self.panel_append_column(panel, 0, 15, "USERNAME")
        self.panel_append_column(panel, 0, 20, "PROGRAM")
        self.panel_append_column(panel, 0, 15, "PGA_ALLOC(MB)")
        self.panel_append_column(panel, 0, 15, "PGA_USED(MB)")
        self.panel_append_column(panel, 0, 15, "PGA_FREE(MB)")

        for idx, row in enumerate(session_pga_detail):
            if idx >= 10:
                break
            self.panel_append_column(panel, idx + 1, 10, row[0])   # SID
            self.panel_append_column(panel, idx + 1, 15, row[1])   # USERNAME
            self.panel_append_column(panel, idx + 1, 20, row[2])   # PROGRAM
            self.panel_append_column(panel, idx + 1, 15, row[3])   # PGA_ALLOC
            self.panel_append_column(panel, idx + 1, 15, row[4])   # PGA_USED
            self.panel_append_column(panel, idx + 1, 15, row[5])   # PGA_FREE

    def refresh_process_info(self):
        """
            Panel3 - Top 10 processes by PGA usage
        """
        panel = self.monitor_panels[3]
        panel["header"] = []
        panel["width"] = []
        panel["value"] = []

        query = """
            SELECT p.PID, p.PROGRAM,
                   ROUND(p.PGA_ALLOC_MEM/1024/1024, 2) AS pga_alloc_mb,
                   ROUND(p.PGA_USED_MEM/1024/1024, 2) AS pga_used_mb,
                   ROUND(p.PGA_FREEABLE_MEM/1024/1024, 2) AS pga_free_mb
            FROM v$process p
            ORDER BY p.PGA_ALLOC_MEM DESC
            FETCH FIRST 10 ROWS ONLY
        """
        process_pga_detail = self.execute_query(query)
        if process_pga_detail is None:
            self.logger.error("Exec process PGA query failed.")
            return

        value_lines = min(len(process_pga_detail), 10)
        panel["value"] = [[] for _ in range(value_lines)]

        # build header
        self.panel_append_column(panel, 0, 10, "PID")
        self.panel_append_column(panel, 0, 30, "PROGRAM")
        self.panel_append_column(panel, 0, 15, "PGA_ALLOC(MB)")
        self.panel_append_column(panel, 0, 15, "PGA_USED(MB)")
        self.panel_append_column(panel, 0, 15, "PGA_FREE(MB)")

        for idx, row in enumerate(process_pga_detail):
            if idx >= 10:
                break
            self.panel_append_column(panel, idx + 1, 10, row[0])   # PID
            self.panel_append_column(panel, idx + 1, 30, row[1])   # PROGRAM
            self.panel_append_column(panel, idx + 1, 15, row[2])   # PGA_ALLOC
            self.panel_append_column(panel, idx + 1, 15, row[3])   # PGA_USED
            self.panel_append_column(panel, idx + 1, 15, row[4])   # PGA_FREE

    def refresh(self):
        self.refresh_summary_info()
        self.refresh_sga_info()

        # check should refresh memory
        if util.should_refresh_memory("memory"):
            self.refresh_session_info()
            self.refresh_process_info()

    def terminate_session_or_thread(self, stdscr):
        cursor_y, cursor_x = stdscr.getyx()
        selected_index = cursor_y - self.begin_y - 1
        if selected_index < 0:
            return

        # terminate confirm
        if not util.terminate_confirm_passed(stdscr):
            return

        panel2 = self.monitor_panels[2]
        panel3 = self.monitor_panels[3]
        panel2_value = panel2["value"]
        panel3_value = panel3["value"]

        if 9 < selected_index <= 9 + len(panel2_value):
            idx = selected_index - (9 + 1)
            selected_row = panel2_value[idx]
            sid = selected_row[0]
            # Look up SERIAL# and terminate
            query = f"SELECT SERIAL# FROM v$session WHERE SID = {sid}"
            result = self.execute_query(query)
            if result and len(result) > 0:
                serial_num = result[0][0]
                self.terminate_session(sid, serial_num)
        elif (9 + len(panel2_value) + 3 < selected_index
              <= 9 + len(panel2_value) + 3 + len(panel3_value)):
            idx = selected_index - (9 + len(panel2_value) + 3 + 1)
            selected_row = panel3_value[idx]
            pid = selected_row[0]
            self.terminate_backend(pid)

    def print(self, stdscr):
        # first clear the screen
        super().clear_screen(stdscr)

        # print header
        start_y = 0
        start_x = self.begin_x

        cursor_y = 0
        cursor_x = 0
        white_bg = None
        if stdscr is not None:
            white_bg = curses.color_pair(2)
            cursor_y, cursor_x = stdscr.getyx()

        for panel_idx, panel in enumerate(self.monitor_panels):
            title = panel["title"]
            header = panel["header"]
            width = panel["width"]
            value = panel["value"]

            # print title
            if title is not None:
                self.printer.addstr(start_y, start_x, f"{title}")
                start_y += 1

            # print header
            self.printer.addstr(start_y, start_x, " " * (self.width - 1), white_bg)
            super().print(stdscr)
            for i, item in enumerate(header):
                data = f"{item}"
                data = data[:width[i] - 1:]
                self.printer.addstr(start_y, start_x, f"{data}", white_bg)
                start_x += width[i]

            # print value
            start_y += 1
            start_x = self.begin_x
            for i, monitor_value_per_line in enumerate(value):
                attr = None
                if not Config.get("main.daemon"):
                    attr = curses.color_pair(1)

                    if self.memory_full_type == EMER_SGA_MEMORY_FULL and \
                            panel_idx == 1:
                        attr = curses.color_pair(5) | curses.A_BOLD

                    if self.memory_full_type == EMER_SESSION_PGA_MEMORY_FULL and \
                            (panel_idx == 2 or panel_idx == 3) and \
                            i < 3:
                        attr = curses.color_pair(5) | curses.A_BOLD

                    # check selected line
                    if self.begin_y + start_y + 1 == cursor_y:
                        attr = attr | curses.A_REVERSE
                        self.printer.addstr(start_y, start_x, " " * (self.width - 1), curses.color_pair(2))
                        super().print(stdscr)

                for j, value in enumerate(monitor_value_per_line):
                    data = f"{value}"
                    data = data[:width[j] - 1:]
                    self.printer.addstr(start_y, start_x, data, attr)
                    start_x += width[j]

                start_y += 1
                start_x = self.begin_x

            self.printer.addstr(start_y, start_x, "")
            start_y += 1

        # print to screen
        super().print(stdscr)
