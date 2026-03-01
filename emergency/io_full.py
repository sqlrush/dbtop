# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
I/O 满载应急模块 (IO Full)

检测 I/O 等待会话数过多，分析 I/O 密集型 SQL 并提供会话终止能力。

检测逻辑:
    - 当处于 USR I/O 状态的活跃会话数超过阈值时触发
    - 分析 I/O 等待会话的 Top SQL，统计 DB Time 和 I/O Time
    - 展示 db file sequential read / db file scattered read 等 I/O 等待细节

交互命令 (按 'k'):
    - 终止选中 SQL_ID 的部分/全部会话
    - 终止执行超时的会话
    - 终止空闲/无事务会话释放资源
"""

from .emergency_base import Emergency
from common.config import Config
from common import alarm, util
import curses
import re

MODULE_NAME = 'IOFull'
MODULE_HEADER = "[EMER05 - IOFull] - select the SQL id and press 'k' to terminate timed-out sessions"
FIRST_PERSIST_NUMBER = 120


class IOFull(Emergency):
    def __init__(self, logger, db_persist):
        super().__init__(MODULE_NAME, MODULE_HEADER, logger, db_persist, FIRST_PERSIST_NUMBER)

    def analyze(self):
        curr_aqu_sz = self.curr_os[11] # see aqu-sz in operating_system.cfg
        if curr_aqu_sz < Config.get("emergency.io_full.io_aqu_sz_thresh"):
            return

        self.analyze_session('USR I/O', Config.get("emergency.io_full.overtime_thresh"))
        self.trigger_emergency(curr_aqu_sz)

    def trigger_emergency(self, curr_aqu_sz):
        if self.sorted_top_sql_dict is None:
            return

        self.emergency_triggered = True
        self.emergency_info.append(f"SQL_ID           ACTIVE_SESS  CPU_PCT  IO_PCT  ANALYZE_CMD           SQL_TEXT")

        top_sql_id = None
        record_print_num = 0
        for sql_id, value in self.sorted_top_sql_dict.items():
            if record_print_num >= 3:
                break

            if record_print_num == 0:
                top_sql_id = sql_id

            active_session_count = value[0]
            sum_db_time = value[1]
            sum_cpu_time = value[2]
            sum_data_io_time = value[3]
            analyze_cmd = value[4][:20]
            query = value[5][:85]

            cpu_time_pct = 0
            io_time_pct = 0
            if sum_db_time != 0:
                cpu_time_pct = round(float(sum_cpu_time)/sum_db_time * 100, 2)
                io_time_pct = round(float(sum_data_io_time)/sum_db_time * 100, 2)

            self.emergency_info.append(f"{sql_id:<15}  {active_session_count:<11}  {cpu_time_pct:<7}  {io_time_pct:<6}  {analyze_cmd:<20}  {query}")
            record_print_num += 1

        # report alarm
        key = f"{MODULE_NAME}"
        io_threshold = Config.get("emergency.io_full.io_aqu_sz_thresh")
        overtime_thresh = Config.get("emergency.io_full.overtime_thresh")
        if len(self.overtime_sess_list) != 0:
            sid_str = ','.join(str(sid) for sid in self.overtime_sess_list)
            command = (f"BEGIN\n"
                       f"    FOR rec IN (SELECT SID, SERIAL# FROM v$session WHERE SID IN ({sid_str})) LOOP\n"
                       f"        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                       f"    END LOOP;\n"
                       f"END;")
            resource_manager_cmd = util.build_resource_manager_cmd(top_sql_id)
            sql_quarantine_cmd = util.build_sql_quarantine_cmd(top_sql_id)

            # print emergency command
            if not Config.get("main.support_terminate"):
                self.emergency_info.append(f"")
                self.append_split_string(command)
                self.append_split_string(resource_manager_cmd)
                self.append_split_string(sql_quarantine_cmd)

            value = (f"DBTOP检测到IO满，当前数据盘的IO平均队列长度：{curr_aqu_sz}，IO满阈值：{io_threshold}，"
                     f"使用以下命令快速查杀在事务内执行时间超过{overtime_thresh}ms且当前活跃会话数较多的占用IO高的会话：{command}，"
                     f"如果查杀异常会话后又不断接入新的请求导致IO冲高，推荐使用Resource Manager限流：{resource_manager_cmd}，"
                     f"极端情况下可以使用SQL Quarantine阻断SQL的执行，对应命令：{sql_quarantine_cmd}")
        else:
            value = f"DBTOP检测到IO满，当前数据盘的IO平均队列长度：{curr_aqu_sz}，IO满阈值：{io_threshold}，打开DBTOP查杀占用IO较多的会话"
        alarm.check_and_report_alarm(self.logger, key, value, True)

    def handle_emergency_command(self, stdscr, command, value):
        # check SQL_ID (Oracle SQL_ID is alphanumeric like 'abc123def')
        match = re.match(r'^\s*(\S+)', value)
        if match:
            sql_id = match.group(1)
        else:
            self.logger.warning("unable to extract sql id, text: %s", value)
            return

        if command == ord('k'):
            stdscr.addstr("Terminate sessions: [1] all active sessions  [2] top X active sessions  [*] Quit\n", curses.color_pair(5) | curses.A_BOLD)
            # set unlimited wait time
            stdscr.timeout(-1)
            # wait input key
            char = stdscr.getch()
            curses.flushinp()
            # terminate confirm
            if char in (ord('1'), ord('2')):
                if not util.terminate_confirm_passed(stdscr):
                    stdscr.timeout(Config.get("main.interval") * 1000)
                    return
            if char == ord('1'):
                self.terminate_unlimited_sessions_withtime(sql_id, Config.get("emergency.io_full.overtime_thresh"))
            elif char == ord('2'):
                stdscr.addstr("Input the number of max terminate sessions: ", curses.color_pair(5) | curses.A_BOLD)
                kill_number = util.get_input_number(stdscr)
                if kill_number > 0:
                    self.terminate_limited_sessions_withtime(sql_id, kill_number, Config.get("emergency.io_full.overtime_thresh"))
            # recover to normal wait time
            stdscr.timeout(Config.get("main.interval") * 1000)
            return
