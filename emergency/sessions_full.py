# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from .emergency_base import Emergency
from common.config import Config
from common import alarm, util
import curses
import re

MODULE_NAME = 'SessionsFull'
MODULE_HEADER = "[EMER06 - SessionsFull] - select the SQL id and press 'k' to terminate sessions"
FIRST_PERSIST_NUMBER = 120


class SessionsFull(Emergency):
    def __init__(self, logger, db_persist):
        super().__init__(MODULE_NAME, MODULE_HEADER, logger, db_persist, FIRST_PERSIST_NUMBER)
        user_whitelist = Config.get("emergency.sessions_full.user_whitelist")
        if user_whitelist is not None and len(user_whitelist) > 0:
            self.whitelist = user_whitelist.split(',')
        self.idle_count = 0
        self.idle_in_xact_count = 0
        self.xact_over_time_count = 0
        self.active_count = 0

    def analyze_session_state(self):
        overtime_interval = Config.get("emergency.sessions_full.overtime_thresh")

        # Get SIDs with open transactions that are INACTIVE (Oracle equivalent of "idle in transaction")
        idle_in_xact_query = """SELECT s.SID FROM v$session s
            JOIN v$transaction t ON s.TADDR = t.ADDR
            WHERE s.TYPE = 'USER' AND s.STATUS = 'INACTIVE'"""
        idle_in_xact_result = self.execute_query(idle_in_xact_query)
        idle_in_xact_sids = set()
        if idle_in_xact_result:
            for row in idle_in_xact_result:
                idle_in_xact_sids.add(row[0])

        # key: sql_id  value: [ idle_num, idle_in_transaction_num, active_and_overtime_num ]
        top_sql_dict = dict()
        for session_row in self.full_session:
            sid = session_row[0]
            sql_id = session_row[4]
            state = session_row[9]
            sql_exec_start = session_row[13]
            xact_run_time = session_row[14]

            top_sql_dict_value = None
            if sql_id is not None and sql_id != '' and sql_id != 0:
                if top_sql_dict.get(sql_id) is None:
                    top_sql_dict[sql_id] = [0] * 3
                top_sql_dict_value = top_sql_dict[sql_id]

            if state == 'INACTIVE':
                if sid in idle_in_xact_sids:
                    self.idle_in_xact_count += 1
                    if top_sql_dict_value is not None:
                        top_sql_dict_value[1] += 1
                else:
                    self.idle_count += 1
                    if top_sql_dict_value is not None:
                        top_sql_dict_value[0] += 1
            elif state == 'ACTIVE' and sql_exec_start is not None:
                self.active_count += 1
                xact_run_time_ms = round(xact_run_time / 1000, 2) if xact_run_time else 0  # "us" -> "ms"
                if xact_run_time_ms >= overtime_interval:
                    self.xact_over_time_count += 1
                    self.emergency_pids.append(sid)
                    if top_sql_dict_value is not None:
                        top_sql_dict_value[2] += 1

        self.sorted_top_sql_dict = dict(sorted(top_sql_dict.items(), key=lambda item: item[1][0], reverse=True))

    def analyze(self):
        cur_proc_data = self.curr_instance[12] # see PROCESSES in instance.cfg
        if cur_proc_data is None or str(cur_proc_data) == 'N/A':
            return

        cur_proc_pct = int(float(str(cur_proc_data).split('%')[0]))
        if cur_proc_pct < Config.get("emergency.sessions_full.sessions_full_thresh"):
            return

        # reset old value
        self.idle_count = 0
        self.idle_in_xact_count = 0
        self.xact_over_time_count = 0
        self.active_count = 0

        self.analyze_session_state()
        self.trigger_emergency(cur_proc_pct)

        # auto terminate
        if Config.get("emergency.sessions_full.terminate") and Config.get("main.support_terminate"):
            self.logger.warning("start terminate sessions because of sessions full")
            self.terminate_none_sessions()
            self.terminate_idle_sessions()
            self.terminate_idle_in_xact_sessions()

    def trigger_emergency(self, cur_proc_pct):
        self.emergency_triggered = True

        self.emergency_info.append(
            f"ACTIVE_SESS: {self.active_count}    IDLE_SESS: {self.idle_count}    IDLE_IN_XACT_SESS: {self.idle_in_xact_count}    XACT_OVERTIME_SESS: {self.xact_over_time_count}")
        self.emergency_info.append(f"SQL_ID           XACT_OVERTIME_SESS            IDLE_SESS        IDLE_IN_XACT_SESS")

        record_print_num = 0
        for sql_id, value in self.sorted_top_sql_dict.items():
            if record_print_num >= 3:
                break

            idle_sess_num = value[0]
            idle_in_transaction_sess_num = value[1]
            active_and_overtime_sess_num = value[2]

            self.emergency_info.append(f"{sql_id:<15}  {active_and_overtime_sess_num:<25}     {idle_sess_num:<15}  {idle_in_transaction_sess_num:<15}")
            record_print_num += 1

        idle_command = ("BEGIN\n"
                        "    FOR rec IN (SELECT SID, SERIAL# FROM v$session WHERE TYPE='USER'\n"
                        "        AND STATUS='INACTIVE' AND WAIT_CLASS='Idle' AND TADDR IS NULL AND ROWNUM <= [LIMIT]) LOOP\n"
                        "        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                        "    END LOOP;\n"
                        "END;")
        idle_in_xact_command = ("BEGIN\n"
                                "    FOR rec IN (SELECT s.SID, s.SERIAL# FROM v$session s\n"
                                "        JOIN v$transaction t ON s.TADDR = t.ADDR\n"
                                "        WHERE s.TYPE='USER' AND s.STATUS='INACTIVE' AND ROWNUM <= [LIMIT]) LOOP\n"
                                "        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                                "    END LOOP;\n"
                                "END;")

        # print emergency command
        if not Config.get("main.support_terminate"):
            self.emergency_info.append(f"")
            self.append_split_string(idle_command)
            self.append_split_string(idle_in_xact_command)

        # report alarm
        key = f"{MODULE_NAME}"
        threshold = Config.get("emergency.sessions_full.sessions_full_thresh")
        value = (f"DBTOP检测到进程/会话满，当前进程使用率：{cur_proc_pct}%，阈值：{threshold}%，"
                 f"使用以下命令快速查杀idle状态的会话（将[LIMIT]替换成需要查杀的最大会话数量）：{idle_command}，"
                 f"如果进程仍然满，则查杀idle in transaction状态的会话：{idle_in_xact_command}")
        alarm.check_and_report_alarm(self.logger, key, value, True)

    def handle_emergency_command(self, stdscr, command, value):
        if command == ord('k'):
            stdscr.addstr("Terminate sessions: [1] None  [2] idle  [3] idle in xact  [4] top X active of selected SQL id  [*] Quit\n", curses.color_pair(5) | curses.A_BOLD)
            # set unlimited wait time
            stdscr.timeout(-1)
            # wait input key
            char = stdscr.getch()
            curses.flushinp()
            # terminate confirm
            if char in (ord('1'), ord('2'), ord('3'), ord('4')):
                if not util.terminate_confirm_passed(stdscr):
                    stdscr.timeout(Config.get("main.interval") * 1000)
                    return
            if char == ord('1'):
                self.terminate_none_sessions()
            elif char == ord('2'):
                self.terminate_idle_sessions()
            elif char == ord('3'):
                self.terminate_idle_in_xact_sessions()
            elif char == ord('4'):
                # check SQL_ID
                match = re.match(r'^\s*(\S+)', value)
                if match:
                    sql_id = match.group(1)
                else:
                    self.logger.warning("unable to extract sql id, text: %s", value)
                    # recover to normal wait time
                    stdscr.timeout(Config.get("main.interval") * 1000)
                    return

                stdscr.addstr("Input the number of max terminate sessions: ", curses.color_pair(5) | curses.A_BOLD)
                kill_number = util.get_input_number(stdscr)
                if kill_number > 0:
                    self.terminate_limited_sessions(sql_id, kill_number)
            # recover to normal wait time
            stdscr.timeout(Config.get("main.interval") * 1000)
            return
