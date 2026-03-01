# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from .monitor_base import Monitor
from common.config import Config
from common import alarm, constants, util
import curses

MONITOR_NAME = 'session'
MONITOR_HEIGHT = 31
MONITOR_LOG_LEVEL = 'WARNING'
MONITOR_CONFIG = "./monitor/session.cfg"

LOCK_HOLDER = "H"
LOCK_WAITER = "W"
LOCK_HOLDER_WAITER = "H&W"

BLK_PRIORITY_MAP = {
    LOCK_HOLDER: 1,
    LOCK_HOLDER_WAITER: 2,
    LOCK_WAITER: 3
}

class SessionMonitor(Monitor):
    def __init__(self):
        super().__init__(MONITOR_NAME, MONITOR_HEIGHT, MONITOR_LOG_LEVEL)
        self.curr_sess_result = None
        self.curr_print_location = 0
        self.curr_pad_length = 0
        self.curr_order_by_col = None
        self.emergency_sql_ids = []
        self.emergency_pids = []
        self.details_height = self.height + 20
        self.details_printer = None

    def init(self, begin_x, begin_y, width):
        super().init(begin_x, begin_y, width)
        self.details_printer = None
        if not Config.get("main.daemon"):
            self.details_printer = curses.newpad(self.details_height, self.width)
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
        # Query PGA memory per session
        query = """
            SELECT s.SID, ROUND(p.PGA_ALLOC_MEM/1024/1024, 2) AS pga_mb
            FROM v$session s
            JOIN v$process p ON s.PADDR = p.ADDR
            WHERE s.TYPE = 'USER'
        """
        mem_result = self.execute_query(query)
        if mem_result is None:
            self.logger.error("Exec PGA query failed.")
            return

        # Query SQL execution stats for soft parse rate
        query = """
            SELECT SQL_ID, EXECUTIONS, PARSE_CALLS
            FROM v$sqlarea
            WHERE EXECUTIONS > 0
        """
        statement_result = self.execute_query(query)
        if statement_result is None:
            self.logger.error("Exec statement query failed.")
            return

        # Main session query
        query = """
        SELECT
            s.SID,
            s.USERNAME,
            s.PROGRAM,
            s.SERIAL#,
            s.SQL_ID,
            (SELECT SUBSTR(sql_text,1,200) FROM v$sqlarea WHERE sql_id=s.SQL_ID AND ROWNUM=1),
            NVL2(s.SQL_ID,
                 (SELECT command_name FROM v$sqlcommand WHERE command_type=s.COMMAND), ''),
            s.BLOCKING_SESSION,
            CASE WHEN s.STATUS='ACTIVE' AND s.SQL_EXEC_START IS NOT NULL
                 THEN ROUND((SYSDATE-s.SQL_EXEC_START)*86400*1000, 2)
                 ELSE 0 END,
            s.STATUS,
            CASE
                WHEN s.STATUS='INACTIVE' OR s.WAIT_CLASS='Idle' THEN 'IDLE'
                WHEN s.WAIT_CLASS IN ('User I/O','System I/O') THEN 'USR I/O'
                WHEN s.WAIT_CLASS IS NULL OR s.STATE='WAITED KNOWN TIME' THEN 'ON CPU'
                ELSE 'WAITING'
            END,
            NVL(s.EVENT, 'ON CPU'),
            s.MACHINE,
            s.SQL_EXEC_START,
            CASE WHEN s.STATUS='ACTIVE' AND s.SQL_EXEC_START IS NOT NULL
                 THEN ROUND((SYSDATE-s.SQL_EXEC_START)*86400*1000000, 2)
                 ELSE 0 END
        FROM v$session s
        WHERE s.TYPE = 'USER'
        ORDER BY s.STATUS
        """

        sess_result = self.execute_query(query)
        if sess_result is None:
            self.logger.error("Exec session query failed.")
            return

        self.handle_sql_result(mem_result, statement_result, sess_result)

    def handle_sql_result(self, mem_result, statement_result, sess_result):
        tmp_monitor_value = []
        tmp_curr_sess_blocker = []
        # save curr sess result
        self.curr_sess_result = sess_result

        mem_result_dict = dict()
        for row in mem_result:
            mem_result_dict[row[0]] = round(row[1], 2)  # key: SID, value: PGA MB

        # calculate soft parse rate
        statement_result_dict = dict()
        for row in statement_result:
            sql_id = row[0]
            executions = row[1]
            parse_calls = row[2]
            if executions != 0 and parse_calls is not None:
                # soft parse rate = (executions - parse_calls) / executions * 100
                soft_parse_rate = round(max(0, (executions - parse_calls) / executions) * 100, 2)
                statement_result_dict[sql_id] = soft_parse_rate
            else:
                statement_result_dict[sql_id] = 0

        for row in sess_result:
            monitor_value_per_line = []
            # row layout:
            # 0: SID, 1: USERNAME, 2: PROGRAM, 3: SERIAL#, 4: SQL_ID, 5: SQL_TEXT
            # 6: OPN, 7: BLOCKING_SESSION, 8: E/T(ms), 9: STATUS
            # 10: STE, 11: EVENT, 12: MACHINE, 13: SQL_EXEC_START, 14: xact_run_time_us
            for col_id, col_value in enumerate(row):
                if col_id == 3:  # SERIAL# -> find "PGA" in mem_result_dict
                    sid = row[0]
                    monitor_value_per_line.append(mem_result_dict.get(sid))
                elif col_id == 5:  # "SQL_TEXT"
                    if col_value is not None and col_value != "":
                        monitor_value_per_line.append(col_value.split('\n')[0])
                    else:
                        monitor_value_per_line.append("")
                elif col_id == 7:  # "BLOCKING_SESSION"
                    if col_value is not None:
                        tmp_curr_sess_blocker.append(col_value)
                    monitor_value_per_line.append(col_value)
                elif col_id == 8:  # "E/T", already in ms from SQL
                    if col_value is not None and col_value != "":
                        monitor_value_per_line.append(round(float(col_value), 2))
                    else:
                        monitor_value_per_line.append(float(0))
                elif col_id == 11:
                    # add "EVENT"
                    monitor_value_per_line.append(col_value)
                    # add "SParse"
                    sql_id = row[4]
                    monitor_value_per_line.append(statement_result_dict.get(sql_id, 0))
                    # add "BLK"
                    monitor_value_per_line.append("")
                    # add SID -> monitor_value_line[14]
                    monitor_value_per_line.append(row[0])
                    # add SERIAL# -> monitor_value_line[15]
                    monitor_value_per_line.append(row[3])
                    # add MACHINE -> monitor_value_line[16]
                    monitor_value_per_line.append(row[12])
                    # add SQL_EXEC_START -> monitor_value_line[17]
                    monitor_value_per_line.append(row[13])
                    # add xact_run_time_us -> monitor_value_line[18]
                    monitor_value_per_line.append(row[14])
                elif col_id >= 12:
                    # skip cols already added above
                    continue
                else:
                    monitor_value_per_line.append(col_value)

            tmp_monitor_value.append(monitor_value_per_line)

        # assign "BLK" and report alarm for block
        if len(tmp_curr_sess_blocker) != 0:
            tmp_monitor_value = self.analyze_block_status(tmp_monitor_value, tmp_curr_sess_blocker)

        # sort
        self.sort_session(tmp_monitor_value)

        with self.lock:
            self.monitor_value = tmp_monitor_value

    def get_session(self):
        return self.curr_sess_result

    def analyze_block_status(self, tmp_monitor_value, tmp_curr_sess_blocker):
        split_line = f"**********************************************************"
        split_line_start = f"{split_line} PRINT BLOCK START {split_line}"
        self.logger.warning(split_line_start)

        # build blocking status map
        blocking_map = {}
        for monitor_value_per_line in tmp_monitor_value:
            sid = monitor_value_per_line[0]
            query = monitor_value_per_line[5]
            blocker = monitor_value_per_line[7]
            state = monitor_value_per_line[9]
            session_sid = monitor_value_per_line[14]
            serial_num = monitor_value_per_line[15]
            sql_exec_start = monitor_value_per_line[17]
            if blocker is not None:
                monitor_value_per_line[13] = LOCK_WAITER
                if sid is not None:
                    if sid in tmp_curr_sess_blocker:
                        monitor_value_per_line[13] = LOCK_HOLDER_WAITER
                    if sid != blocker:
                        blocking_map[sid] = blocker
            else:
                if sid is not None and sid in tmp_curr_sess_blocker:
                    monitor_value_per_line[13] = LOCK_HOLDER
                    key = f"BLOCKER_{session_sid}"
                    command = f"ALTER SYSTEM KILL SESSION '{session_sid},{serial_num}' IMMEDIATE;"
                    value = f"DBTOP检测到锁阻塞，阻塞源头会话SID：{session_sid}，SERIAL#：{serial_num}，使用以下命令快速查杀会话：{command}"
                    alarm.check_and_report_alarm(self.logger, key, value, True)

            # write to log
            blk = monitor_value_per_line[13]
            if blk != "":
                self.logger.warning(
                    f"BLK: {blk} SID: {session_sid} SERIAL#: {serial_num} BLOCKER: {blocker} STATE: {state} SQL_EXEC_START: {sql_exec_start} QUERY: {query}")

        deadlocks = []
        visited_all = set()

        def find_cycle(start_sid):
            visited_local = {}
            current = start_sid
            step = 0

            while current in blocking_map:
                if current in visited_local:
                    cycle_start_idx = visited_local[current]

                    cycle = []
                    for node, idx in visited_local.items():
                        if idx >= cycle_start_idx:
                            cycle.append((idx, node))

                    cycle.sort(key=lambda x: x[0])
                    nodes_in_cycle = [node for _, node in cycle]
                    nodes_in_cycle.append(nodes_in_cycle[0])
                    return nodes_in_cycle

                if current in visited_all:
                    return None

                visited_local[current] = step
                step += 1
                current = blocking_map[current]

            return None

        for sid in blocking_map:
            if sid not in visited_all:
                deadlock = find_cycle(sid)
                if deadlock:
                    deadlocks.append(deadlock)
                    for node in deadlock[:-1]:
                        visited_all.add(node)

        if deadlocks:
            self.logger.warning(f"FOUND {len(deadlocks)} DEADLOCKS")
            for i, deadlock in enumerate(deadlocks, 1):
                for j in range(len(deadlock) - 1):
                    self.logger.warning(f"DEADLOCK[{i}]: {deadlock[j]} -> {deadlock[j + 1]}")

        split_line_end = f"{split_line} PRINT BLOCK END {split_line}"
        self.logger.warning(split_line_end)
        return tmp_monitor_value

    def sort_session(self, monitor_value_list):
        if self.curr_order_by_col is not None:
            if self.curr_order_by_col == 11:
                monitor_value_list.sort(key=lambda x: \
                    "" if x[self.curr_order_by_col] is None else x[self.curr_order_by_col], reverse=True)
            else:
                monitor_value_list.sort(key=lambda x: x[self.curr_order_by_col], reverse=True)

        # sort by "BLK"
        monitor_value_list.sort(key=lambda x: BLK_PRIORITY_MAP.get(x[13], 999))

    # sort by "PGA"
    def refresh_by_pga(self):
        self.curr_order_by_col = 3
        with self.lock:
            self.sort_session(self.monitor_value)

    # sort by "E/T"
    def refresh_by_elapsed_time(self):
        self.curr_order_by_col = 8
        with self.lock:
            self.sort_session(self.monitor_value)

    # sort by "EVENT"
    def refresh_by_event(self):
        self.curr_order_by_col = 11
        with self.lock:
            self.sort_session(self.monitor_value)

    # set by "plan change" emergency module
    def set_trigger_emergency_sql_ids(self, trigger_emergency_sql_ids):
        self.emergency_sql_ids = trigger_emergency_sql_ids

    # set by "cpu full" "io full" "sessions full" emergency module
    def set_trigger_emergency_pids(self, trigger_emergency_pids):
        self.emergency_pids = trigger_emergency_pids

    def reset_print_location(self):
        self.curr_print_location = 0
        return

    def get_pad_length(self):
        return self.curr_pad_length

    def terminate_selected_session(self, stdscr):
        selected_index = self.get_selected_location(stdscr)
        if selected_index < 0:
            return

        # terminate confirm
        if not util.terminate_confirm_passed(stdscr):
            return

        monitor_value_line = self.monitor_value[selected_index]
        sid = monitor_value_line[14]
        serial_num = monitor_value_line[15]
        self.terminate_session(sid, serial_num)

    def terminate_all_sessions(self, stdscr):
        selected_index = self.get_selected_location(stdscr)
        if selected_index < 0:
            return

        # terminate confirm
        if not util.terminate_confirm_passed(stdscr):
            return

        monitor_value_line = self.monitor_value[selected_index]
        sql_id = monitor_value_line[4]
        if sql_id is None or sql_id == '' or sql_id == 0:
            self.logger.error("Cannot terminate the sessions with SQL ID is empty")
            return
        command = constants.TERMINATE_UNLIMITED_SESSIONS_ANONYMOUS_BLOCK.format(sql_id=sql_id)
        self.execute_noreturn_query(command)

    def get_selected_location(self, stdscr):
        cursor_y, cursor_x = stdscr.getyx()
        return cursor_y - self.begin_y - 1 + self.curr_print_location

    def check_highlight_location(self, pageup_or_pagedown, step = MONITOR_HEIGHT):
        min_cur_loc = self.curr_print_location + pageup_or_pagedown * step
        if pageup_or_pagedown == -1 and self.curr_print_location == 0 or min_cur_loc >= len(self.monitor_value):
            self.logger.debug("Printer location out of range.")
            return False
        self.curr_print_location = max(min_cur_loc, 0)
        return True

    def print(self, stdscr):
        # clear the screen
        super().clear_screen(stdscr)

        # print header
        start_y = 0
        start_x = self.begin_x
        white_bg = None
        if stdscr is not None:
            white_bg = curses.color_pair(2)
        for i, item in enumerate(self.monitor_item):
            self.printer.addstr(start_y, start_x, f"{item}", white_bg)
            start_x += self.monitor_width[i]

        # print monitor value
        start_y = 1
        start_x = self.begin_x
        self.curr_pad_length = 0
        for i, monitor_value_per_line in enumerate(self.monitor_value[self.curr_print_location:]):
            # print limit lines
            if i == (MONITOR_HEIGHT - 1):
                break

            print_attr = None
            if stdscr is not None:
                print_attr = curses.color_pair(1)

                # check lock
                blk = monitor_value_per_line[13]
                if blk == LOCK_WAITER:
                    print_attr = curses.color_pair(6) | curses.A_BOLD
                elif blk == LOCK_HOLDER:
                    print_attr = curses.color_pair(3) | curses.A_BOLD
                elif blk == LOCK_HOLDER_WAITER:
                    print_attr = curses.color_pair(7) | curses.A_BOLD

                # check emergency sql
                sid = monitor_value_per_line[0]
                if len(self.emergency_sql_ids) != 0:
                    sql_id = monitor_value_per_line[4]
                    if sql_id in self.emergency_sql_ids:
                        print_attr = curses.color_pair(5) | curses.A_BOLD
                elif len(self.emergency_pids) != 0:
                    if sid in self.emergency_pids:
                        print_attr = curses.color_pair(5) | curses.A_BOLD

                # check selected line
                cursor_y, cursor_x = stdscr.getyx()
                if self.begin_y + start_y == cursor_y:
                    print_attr = print_attr | curses.A_REVERSE
                    self.printer.addstr(start_y, start_x, " " * (self.width - 1), white_bg)
                    # fix printed selected line not correct format
                    super().print(stdscr)

            # print limited data
            for idx, item in enumerate(self.monitor_item):
                monitor_value = f"{monitor_value_per_line[idx]}"
                data = monitor_value[:self.monitor_width[idx] - 1:]
                self.printer.addstr(start_y, start_x, data, print_attr)
                start_x += self.monitor_width[idx]

            # continue to next line
            start_y += 1
            start_x = self.begin_x
            self.curr_pad_length += 1

        # print to screen
        super().print(stdscr)

    def print_string_to_pad(self, stdscr, start_y, start_x, string, attr=None):
        if start_y >= self.height:
            return
        if attr is not None:
            self.printer.addstr(start_y, start_x, string, attr)
        else:
            self.printer.addstr(start_y, start_x, string)

        # print to screen
        super().print(stdscr)
        return start_y + 1

    def print_execute_plan(self, stdscr, session_sid, sql_id):
        new_pad_height = self.details_height
        new_pad = curses.newpad(new_pad_height, self.width)
        origin_printer = self.printer_switch((new_pad_height, new_pad))

        start_y = 0
        start_x = self.begin_x

        # print header
        header_str = "SESSION DETAILS"
        start_y = self.print_string_to_pad(stdscr, start_y, start_x, header_str + " " * (self.width - 1 - len(header_str)), curses.color_pair(2))

        # print execution plan using DBMS_XPLAN
        start_y = self.print_string_to_pad(stdscr, start_y, start_x, "[THE EXECUTION PLAN]")
        if sql_id is not None and sql_id != '' and sql_id != 0:
            sql_query = f"SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('{sql_id}', NULL, 'ALLSTATS LAST'))"
            result = self.execute_query(sql_query)
            if result is None or len(result) == 0:
                self.logger.error(f"Exec query DBMS_XPLAN for sql_id={sql_id} failed.")
                start_y = self.print_string_to_pad(stdscr, start_y, start_x, "No recorded query plan in DBMS_XPLAN.")
            else:
                for i, row in enumerate(result):
                    if i >= 20:
                        break
                    line = str(row[0]) if row[0] is not None else ""
                    start_y = self.print_string_to_pad(stdscr, start_y, start_x, line)

        # wait
        start_y = self.print_string_to_pad(stdscr, start_y, start_x, "")
        start_y = self.print_string_to_pad(stdscr, start_y, start_x, "Press any key to continue...")
        char = stdscr.getch()
        curses.flushinp()

        # switch to origin printer and refresh screen
        self.printer_switch(origin_printer)
        super().print(stdscr)

    def print_detail_sql_text(self, stdscr, session_sid):
        new_pad_height = self.details_height
        new_pad = curses.newpad(new_pad_height, self.width)
        origin_printer = self.printer_switch((new_pad_height, new_pad))

        start_y = 0
        start_x = self.begin_x

        # print header
        header_str = "SESSION DETAILS"
        start_y = self.print_string_to_pad(stdscr, start_y, start_x, header_str + " " * (self.width - 1 - len(header_str)), curses.color_pair(2))

        # print sql text
        start_y = self.print_string_to_pad(stdscr, start_y, start_x, "[THE SQL TEXT]")

        sql_text = self.get_sql_full_text_by_sid(session_sid)
        if sql_text is not None and len(sql_text) != 0:
            for line in sql_text.split('\n'):
                start_y = self.print_string_to_pad(stdscr, start_y, start_x, line)

        # wait
        start_y = self.print_string_to_pad(stdscr, start_y, start_x, "")
        start_y = self.print_string_to_pad(stdscr, start_y, start_x, "Press any key to continue...")
        char = stdscr.getch()
        curses.flushinp()

        # switch to origin printer and refresh screen
        self.printer_switch(origin_printer)
        super().print(stdscr)

    def printer_switch(self, printer_tuple):
        origin_height = self.height
        origin_printer = self.printer
        self.height = printer_tuple[0]
        self.printer = printer_tuple[1]
        return origin_height, origin_printer

    def print_more_details(self, stdscr):
        selected_index = self.get_selected_location(stdscr)
        if selected_index < 0:
            return

        # switch to the details printer
        origin_printer = self.printer_switch((self.details_height, self.details_printer))

        def print_string_to_pad(string, attr=None):
            nonlocal start_y
            nonlocal start_x
            if start_y >= self.height:
                return
            if attr is not None:
                self.printer.addstr(start_y, start_x, string, attr)
            else:
                self.printer.addstr(start_y, start_x, string)
            start_y += 1
            super(type(self), self).print(stdscr)

        def terminate_confirm_passed():
            nonlocal start_y
            result = False
            confirm_str = "Confirm again whether you need to execute the terminate command (y/n): "
            print_string_to_pad(confirm_str)

            while True:
                input_char = stdscr.getch()
                curses.flushinp()

                if input_char in [10, 13]:
                    return result
                elif input_char == ord('y'):
                    result = True
                else:
                    result = False

                self.printer.addstr(start_y - 1, len(confirm_str), chr(input_char))
                super(type(self), self).print(stdscr)

        # set unlimited wait time
        stdscr.timeout(-1)

        while True:
            # clear screen first
            super().clear_screen(stdscr)

            start_y = 0
            start_x = self.begin_x

            # print header
            header_str = "SESSION DETAILS"
            print_string_to_pad(header_str + " " * (self.width - 1 - len(header_str)), curses.color_pair(2))

            # print details
            monitor_value_line = self.monitor_value[selected_index]
            user_name = monitor_value_line[1]
            sql_id = monitor_value_line[4]
            sql_text = monitor_value_line[5][:110]
            session_sid = monitor_value_line[14]
            serial_num = monitor_value_line[15]
            machine = monitor_value_line[16]
            sql_exec_start = monitor_value_line[17]
            print_string_to_pad(f"USERNAME: {user_name}    MACHINE: {machine}")
            print_string_to_pad(f"SID: {session_sid}    SERIAL#: {serial_num}    SQL_EXEC_START: {sql_exec_start}")
            print_string_to_pad(f"SQL_ID: {sql_id}    SQL_TEXT: {sql_text}")
            print_string_to_pad("")

            # print blocked tree
            start_y, lock_holder, sql_sid_array = self.print_blocked_tree(session_sid, start_y)
            if lock_holder is None:
                print_string_to_pad("")

            print_string_to_pad(f"[SUPPORT COMMANDS]")
            print_string_to_pad(f"  [1] Print the full SQL text")
            print_string_to_pad(f"  [2] Print the execution plan")
            if Config.get("main.support_terminate"):
                print_string_to_pad(f"  [3] Terminate single selected session")
                print_string_to_pad(f"  [4] Terminate part of sessions with same SQL id")
                print_string_to_pad(f"  [5] Terminate all sessions with same SQL id")
            if lock_holder is not None:
                if Config.get("main.support_terminate"):
                    print_string_to_pad(f"  [6] Terminate the blocker session '{lock_holder}'")
                print_string_to_pad(f"  [7] Print the full SQL text of block tree")
            print_string_to_pad(f"  [*] Quit")
            print_string_to_pad("")

            # wait input key
            char = stdscr.getch()
            curses.flushinp()
            if char == ord('1'):
                self.print_detail_sql_text(stdscr, session_sid)
            elif char == ord('2'):
                self.print_execute_plan(stdscr, session_sid, sql_id)
            elif char == ord('3') and Config.get("main.support_terminate"):
                if terminate_confirm_passed():
                    self.terminate_session(session_sid, serial_num)
            elif char == ord('4') and Config.get("main.support_terminate"):
                if sql_id is None or sql_id == '' or sql_id == 0:
                    print_string_to_pad(f"Cannot terminate the sessions with SQL ID is empty, press any key to continue...")
                    char = stdscr.getch()
                    curses.flushinp()
                    continue
                if terminate_confirm_passed():
                    hint_str = "Input the number of max terminate sessions: "
                    print_string_to_pad(hint_str)
                    stdscr.move(self.begin_y + start_y - 1, len(hint_str))
                    terminate_number = util.get_input_number(stdscr)
                    if terminate_number > 0:
                        anonymous_block = constants.TERMINATE_LIMITED_SESSIONS_ANONYMOUS_BLOCK.format(
                            sql_id=sql_id, max_terminate_count=terminate_number)
                        self.execute_noreturn_query(anonymous_block)
            elif char == ord('5') and Config.get("main.support_terminate"):
                if sql_id is None or sql_id == '' or sql_id == 0:
                    print_string_to_pad(f"Cannot terminate the sessions with SQL ID is empty, press any key to continue...")
                    char = stdscr.getch()
                    curses.flushinp()
                    continue
                if terminate_confirm_passed():
                    command = constants.TERMINATE_UNLIMITED_SESSIONS_ANONYMOUS_BLOCK.format(sql_id=sql_id)
                    self.execute_noreturn_query(command)
            elif char == ord('6') and Config.get("main.support_terminate"):
                if lock_holder is not None and terminate_confirm_passed():
                    if self.terminate_blocker_session(session_sid, lock_holder):
                        print_string_to_pad(
                            f"Terminate the blocked session '{lock_holder}' succeed, press any key to continue...")
                    else:
                        print_string_to_pad(
                            f"Terminate the blocked session '{lock_holder}' failed, press any key to continue...")
                    char = stdscr.getch()
                    curses.flushinp()
            elif char == ord('7'):
                if lock_holder is not None:
                    hint_str = "Input the SQL number: "
                    print_string_to_pad(hint_str)
                    stdscr.move(self.begin_y + start_y - 1, len(hint_str))
                    sql_sid_idx = util.get_input_number(stdscr)
                    if sql_sid_idx < len(sql_sid_array):
                        self.print_detail_sql_text(stdscr, sql_sid_array[sql_sid_idx])
            else:
                # clear screen
                super().clear_screen(stdscr)
                # recover to normal wait time
                stdscr.timeout(Config.get("main.interval") * 1000)
                self.printer_switch(origin_printer)
                return

    def get_blocking_session_info(self, target_sid):
        """Query v$session for blocking info of a specific SID"""
        query = f"SELECT SID, BLOCKING_SESSION, EVENT, WAIT_CLASS FROM v$session WHERE SID = {target_sid}"
        result = self.execute_query(query)
        if result is None:
            self.logger.error(f"Exec query failed for SID: {target_sid}")
            return None
        if len(result) == 0:
            return None
        return result[0]

    def get_blocker_by_sid(self, target_sid):
        """Get the SID of the session blocking the target"""
        query = f"SELECT BLOCKING_SESSION FROM v$session WHERE SID = {target_sid}"
        result = self.execute_query(query)
        if result is None:
            self.logger.error(f"Exec query failed for SID: {target_sid}")
            return None
        if len(result) == 0:
            return None
        return result[0][0]

    def get_lockinfo_by_sid(self, target_sid):
        """Query lock info for sessions waiting on or holding locks of target SID"""
        query = f"""
            SELECT l.SID, l.TYPE, l.LMODE, l.REQUEST, l.ID1, l.ID2,
                   s.SERIAL#, s.SQL_ID,
                   (SELECT SUBSTR(sql_text,1,200) FROM v$sqlarea WHERE sql_id=s.SQL_ID AND ROWNUM=1) AS sql_text
            FROM v$lock l
            JOIN v$session s ON l.SID = s.SID
            WHERE (l.ID1, l.ID2) IN (
                SELECT ID1, ID2 FROM v$lock WHERE SID = {target_sid} AND LMODE > 0
            )
        """
        result = self.execute_query(query)
        if result is None:
            self.logger.error(f"Exec lock query failed for SID: {target_sid}")
            return None
        return result

    def get_sql_text_by_sid(self, target_sid):
        for monitor_value_per_line in self.monitor_value:
            sid = monitor_value_per_line[0]
            if sid == target_sid:
                return monitor_value_per_line[5]

    def get_sql_full_text_by_sid(self, target_sid):
        if self.curr_sess_result is None:
            return None
        for row in self.curr_sess_result:
            sid = row[0]
            if sid == target_sid:
                return row[5]

    def print_blocked_tree(self, target_sid, start_y):
        def print_string_to_pad(string, attr=None, prefix=None):
            nonlocal start_y
            start_x = self.begin_x
            if start_y >= self.height:
                return
            if prefix is not None:
                self.printer.addstr(start_y, start_x, prefix)
                start_x += len(prefix) + 1

            if string is not None:
                if attr is not None:
                    self.printer.addstr(start_y, start_x, string, attr)
                else:
                    self.printer.addstr(start_y, start_x, string)
            start_y += 1

        # print the block tree
        print_string_to_pad(f"[THE BLOCK TREE]")
        sql_sid_array = []
        lock_holder = None

        # Check if this session is blocked
        block_info = self.get_blocking_session_info(target_sid)
        if block_info is not None:
            blocking_session = block_info[1]

            if blocking_session is not None:
                # Get lock info for the blocker
                lock_info = self.get_lockinfo_by_sid(blocking_session)

                if lock_info is not None:
                    lock_waiter = []
                    for row in lock_info:
                        sid = row[0]
                        lock_type = row[1]
                        lmode = row[2]
                        request = row[3]
                        id1 = row[4]
                        id2 = row[5]
                        serial_num = row[6]
                        sql_id = row[7]
                        sql_text = row[8]

                        if lmode > 0 and request == 0:  # lock holder
                            lock_holder = sid
                            sql_display = str(sql_text)[:140] if sql_text else ""
                            print_string_to_pad(f"SID: {sid}  SERIAL#: {serial_num}")
                            print_string_to_pad(f"Lock type: {lock_type}  Lock mode: {lmode}  ID1: {id1}  ID2: {id2}")
                            print_string_to_pad(f"SQL[{len(sql_sid_array)}]: {sql_display}")
                            sql_sid_array.append(sid)
                        elif request > 0:  # lock waiter
                            waiter = dict()
                            waiter["sid"] = sid
                            waiter["serial"] = serial_num
                            waiter["mode"] = request
                            waiter["sql_text"] = sql_text
                            lock_waiter.append(waiter)

                    for i, waiter in enumerate(lock_waiter):
                        if i == 5:
                            print_string_to_pad(None, None, f"    |    ")
                            print_string_to_pad("<all %d sessions>" % len(lock_waiter), None, f"    |----")
                            break
                        sid = waiter["sid"]
                        mode = waiter["mode"]
                        sql = str(waiter["sql_text"])[:80] if waiter["sql_text"] else ""
                        print_attr = curses.color_pair(5) if sid == target_sid else None
                        print_string_to_pad(None, None, f"    |    ")
                        print_string_to_pad(f"SID: {sid}  Lock mode: {mode}  SQL[{len(sql_sid_array)}]: {sql}", print_attr, f"    |----")
                        sql_sid_array.append(sid)
                    # blank line
                    print_string_to_pad(None)

        # print the block tree that is blocked by the current session
        lock_info = self.get_lockinfo_by_sid(target_sid)
        if lock_info is None:
            return start_y, lock_holder, sql_sid_array

        all_lock_info = dict()
        for row in lock_info:
            sid = row[0]
            lock_type = row[1]
            lmode = row[2]
            request = row[3]
            id1 = row[4]
            id2 = row[5]
            serial_num = row[6]
            sql_id = row[7]
            sql_text = row[8]

            lock_key = f"{id1}_{id2}"
            if lock_key not in all_lock_info:
                all_lock_info[lock_key] = []
            all_lock_info[lock_key].append({
                "sid": sid, "locktype": lock_type, "lmode": lmode,
                "request": request, "serial": serial_num,
                "sql_id": sql_id, "sql_text": sql_text
            })

        for lock_key, lock_records in all_lock_info.items():
            lock_holder_record = None
            block_other_session = False
            for lock_record in lock_records:
                if lock_record["request"] > 0:
                    block_other_session = True
                elif lock_record["lmode"] > 0:
                    lock_holder_record = lock_record

            if not block_other_session:
                continue

            if lock_holder_record is None:
                continue

            if lock_holder is None:
                lock_holder = lock_holder_record["sid"]

            sql = str(lock_holder_record["sql_text"])[:140] if lock_holder_record["sql_text"] else ""
            print_attr = curses.color_pair(5) if lock_holder_record["sid"] == target_sid else None
            print_string_to_pad("SID: " + str(lock_holder_record["sid"]) + "  SERIAL#: " + str(lock_holder_record["serial"]), print_attr)
            print_string_to_pad("Lock type: " + lock_holder_record["locktype"] +
                                "  Lock mode: " + str(lock_holder_record["lmode"]) +
                                "  Lock key: " + lock_key, print_attr)
            print_string_to_pad(f"SQL[{len(sql_sid_array)}]: {sql}", print_attr)
            sql_sid_array.append(lock_holder_record["sid"])

            lock_waiter_print_num = 0
            for lock_record in lock_records:
                if lock_record["request"] > 0:
                    if lock_waiter_print_num == 5:
                        print_string_to_pad(None, None, f"    |    ")
                        print_string_to_pad("<all %d sessions>" % (len(lock_records) - 1), None, f"    |----")
                        break
                    sid = lock_record["sid"]
                    serial = lock_record["serial"]
                    mode = lock_record["request"]
                    sql = str(lock_record["sql_text"])[:80] if lock_record["sql_text"] else ""
                    print_string_to_pad(None, None, f"    |    ")
                    print_string_to_pad(f"SID: {sid}  Lock mode: {mode}  SQL[{len(sql_sid_array)}]: {sql}", None, f"    |----")
                    lock_waiter_print_num += 1
                    sql_sid_array.append(sid)
            # blank line
            print_string_to_pad(None)

        return start_y, lock_holder, sql_sid_array

    def terminate_blocker_session(self, session_sid, lock_holder):
        self.logger.warning(f"start terminate lock holder: session_sid={session_sid} lock_holder={lock_holder}.")
        if str(session_sid) == str(lock_holder):
            # Look up SERIAL# for the blocker
            query = f"SELECT SERIAL# FROM v$session WHERE SID = {lock_holder}"
            result = self.execute_query(query)
            if result and len(result) > 0:
                serial_num = result[0][0]
                command = f"ALTER SYSTEM KILL SESSION '{lock_holder},{serial_num}' IMMEDIATE"
                return self.execute_noreturn_query(command)
            return False

        blocker = self.get_blocker_by_sid(session_sid)
        if blocker is None or blocker != lock_holder:
            return False

        # Look up SERIAL# for the blocker
        query = f"SELECT SERIAL# FROM v$session WHERE SID = {blocker}"
        result = self.execute_query(query)
        if result and len(result) > 0:
            serial_num = result[0][0]
            command = f"ALTER SYSTEM KILL SESSION '{blocker},{serial_num}' IMMEDIATE"
            return self.execute_noreturn_query(command)
        return False
