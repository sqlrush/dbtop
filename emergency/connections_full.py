# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from .emergency_base import Emergency
from common.config import Config
from common import alarm, util
import curses

MODULE_NAME = 'ConnectionsFull'
MODULE_HEADER = "[EMER07 - ConnectionsFull] - select the username and press 'k' to terminate sessions"
FIRST_PERSIST_NUMBER = 120


class ConnectionsFull(Emergency):
    def __init__(self, logger, db_persist):
        super().__init__(MODULE_NAME, MODULE_HEADER, logger, db_persist, FIRST_PERSIST_NUMBER)
        user_whitelist = Config.get("emergency.connections_full.user_whitelist")
        if user_whitelist is not None and len(user_whitelist) > 0:
            self.whitelist = user_whitelist.split(',')
        self.sorted_top_sql_dict = None

    def analyze_session_state(self):
        # Get SIDs with open transactions that are INACTIVE (Oracle equivalent of "idle in transaction")
        idle_in_xact_query = """SELECT s.SID FROM v$session s
            JOIN v$transaction t ON s.TADDR = t.ADDR
            WHERE s.TYPE = 'USER' AND s.STATUS = 'INACTIVE'"""
        idle_in_xact_result = self.execute_query(idle_in_xact_query)
        idle_in_xact_sids = set()
        if idle_in_xact_result:
            for row in idle_in_xact_result:
                idle_in_xact_sids.add(row[0])

        # key: usename  value: [ total_sess_num, idle_sess_num, idle_in_xact_sess_num, active_sess_num, none_num ]
        top_sql_dict = dict()
        for session_row in self.full_session:
            usename = session_row[1]
            sid = session_row[0]
            state = session_row[9]

            if top_sql_dict.get(usename) is None:
                top_sql_dict[usename] = [ 0, 0, 0, 0, 0 ]

            top_sql_dict[usename][0] += 1
            if state == 'INACTIVE':
                if sid in idle_in_xact_sids:
                    top_sql_dict[usename][2] += 1
                else:
                    top_sql_dict[usename][1] += 1
            elif state == 'ACTIVE':
                top_sql_dict[usename][3] += 1
            elif state is None:
                top_sql_dict[usename][4] += 1

        self.sorted_top_sql_dict = dict(sorted(top_sql_dict.items(), key=lambda item: item[1][0], reverse=True))

    def analyze(self):
        cur_conn_data = self.curr_instance[11] # see CONNECTION(c/m) in instance.cfg
        if cur_conn_data is None or str(cur_conn_data) == 'N/A':
            return

        percent_part, fraction_part = str(cur_conn_data).split('%') # conn data like '50%(5000/10000)'
        cur_conn_pct = int(float(percent_part))
        if cur_conn_pct < Config.get("emergency.connections_full.connections_full_thresh"):
            return

        fraction = fraction_part.strip('()')
        cur_conn_str, max_conn_str = fraction.split('/')
        cur_conn = int(cur_conn_str)
        max_conn = int(max_conn_str)

        # reset old value
        self.sorted_top_sql_dict = None

        self.analyze_session_state()
        self.trigger_emergency(cur_conn_pct, cur_conn, max_conn)

        # auto terminate
        if Config.get("emergency.connections_full.terminate") and Config.get("main.support_terminate"):
            self.logger.warning("start terminate sessions because of connections full")
            self.terminate_none_sessions()
            self.terminate_idle_sessions()
            self.terminate_idle_in_xact_sessions()

    def trigger_emergency(self, cur_conn_pct, cur_conn, max_conn):
        self.emergency_triggered = True
        self.emergency_info.append(
            f"USER                     TOTAL_SESS           IDLE_SESS         IDLE_IN_XACT_SESS       ACTIVE_SESS    NONE_SESS")

        record_print_num = 0
        for username, value in self.sorted_top_sql_dict.items():
            if record_print_num >= 3:
                break
            self.emergency_info.append(
                f"{username:<24} {value[0]:<20} {value[1]:<17} {value[2]:<23} {value[3]:<14} {value[4]}")
            record_print_num += 1

        command = ("BEGIN\n"
                   "    FOR rec IN (SELECT SID, SERIAL# FROM v$session\n"
                   "        WHERE TYPE='USER' AND STATUS='INACTIVE' AND ROWNUM <= [LIMIT]) LOOP\n"
                   "        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                   "    END LOOP;\n"
                   "END;")

        # print emergency command
        if not Config.get("main.support_terminate"):
            self.emergency_info.append(f"")
            self.append_split_string(command)

        # report alarm
        key = f"{MODULE_NAME}"
        threshold = Config.get("emergency.connections_full.connections_full_thresh")
        value = (f"DBTOP检测到连接数满，当前连接数使用率：{cur_conn_pct}%，连接数满阈值：{threshold}%，当前连接数：{cur_conn}，最大连接数：{max_conn}，"
                 f"使用以下命令快速查杀所有空闲会话（将[LIMIT]替换成需要查杀的最大会话数量）：{command}")
        alarm.check_and_report_alarm(self.logger, key, value, True)

    def terminate_idle_sessions_with_name(self, username):
        plsql = (f"BEGIN\n"
                 f"    FOR rec IN (SELECT SID, SERIAL# FROM v$session\n"
                 f"        WHERE STATUS='INACTIVE' AND WAIT_CLASS='Idle' AND TADDR IS NULL AND USERNAME='{username}') LOOP\n"
                 f"        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                 f"    END LOOP;\n"
                 f"END;")
        self.execute_noreturn_query(plsql)

    def terminate_idle_in_xact_sessions_with_name(self, username):
        plsql = (f"BEGIN\n"
                 f"    FOR rec IN (SELECT s.SID, s.SERIAL# FROM v$session s\n"
                 f"        JOIN v$transaction t ON s.TADDR = t.ADDR\n"
                 f"        WHERE s.STATUS='INACTIVE' AND s.USERNAME='{username}') LOOP\n"
                 f"        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                 f"    END LOOP;\n"
                 f"END;")
        self.execute_noreturn_query(plsql)

    def terminate_none_sessions_with_name(self, username):
        plsql = (f"BEGIN\n"
                 f"    FOR rec IN (SELECT SID, SERIAL# FROM v$session\n"
                 f"        WHERE STATUS='INACTIVE' AND SQL_ID IS NULL AND TADDR IS NULL AND USERNAME='{username}') LOOP\n"
                 f"        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                 f"    END LOOP;\n"
                 f"END;")
        self.execute_noreturn_query(plsql)

    def terminate_top_active_sessions(self, username, limit):
        plsql = (f"DECLARE\n"
                 f"    v_count NUMBER := 0;\n"
                 f"BEGIN\n"
                 f"    FOR rec IN (SELECT SID, SERIAL# FROM v$session\n"
                 f"        WHERE STATUS='ACTIVE' AND USERNAME='{username}' ORDER BY SQL_EXEC_START NULLS LAST) LOOP\n"
                 f"        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                 f"        v_count := v_count + 1;\n"
                 f"        EXIT WHEN v_count >= {limit};\n"
                 f"    END LOOP;\n"
                 f"END;")
        self.execute_noreturn_query(plsql)

    def handle_emergency_command(self, stdscr, command, value):
        if command == ord('k'):
            username = value.split()[0]
            if username is None or len(username) == 0 or username == 'USER': # invalid case or select head
                return
            stdscr.addstr("Terminate sessions: [1] None  [2] idle  [3] idle in xact  [4] top X active of selected user  [*] Quit\n", curses.color_pair(5) | curses.A_BOLD)
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
                self.terminate_none_sessions_with_name(username)
            elif char == ord('2'):
                self.terminate_idle_sessions_with_name(username)
            elif char == ord('3'):
                self.terminate_idle_in_xact_sessions_with_name(username)
            elif char == ord('4'):
                stdscr.addstr("Input the number of max terminate sessions: ", curses.color_pair(5) | curses.A_BOLD)
                kill_number = util.get_input_number(stdscr)
                if kill_number > 0:
                    self.terminate_top_active_sessions(username, kill_number)
            # recover to normal wait time
            stdscr.timeout(Config.get("main.interval") * 1000)
            return
