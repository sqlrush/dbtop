# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

import re
from abc import ABC, abstractmethod
from datetime import datetime
from common.config import Config
from common import constants, log, util
import os

SESSION_TIME_SQL = """SELECT s.SID, st.STAT_NAME, st.VALUE
FROM v$sess_time_model st
JOIN v$session s ON st.SID = s.SID
WHERE s.TYPE = 'USER'
  AND st.STAT_NAME IN ('DB time', 'DB CPU', 'db file sequential read time', 'db file scattered read time')"""


class Emergency:
    def __init__(self, name, header, logger, db_persist, snapshot_persist_number):
        self.name = name
        self.header = header
        # save args
        self.logger = logger
        self.db_persist = db_persist
        # save db_id
        self.db_id = 0
        # init value
        self.curr_snap_id = 0
        self.curr_snap_ts = 0
        self.curr_db = None
        self.curr_os = None
        self.curr_instance = None
        self.curr_event = None
        self.curr_session = None # processed session data printed on the screen
        self.full_session = None # full session data retrieved by the SQL query
        self.curr_memory = None
        # init emergency info
        self.emergency_triggered = False
        self.emergency_info = []
        self.emergency_sql_ids = []
        self.emergency_pids = []
        # init log persist parameter
        self.need_persist = False
        self.start_persist_snap_id = 0
        self.start_persist_time = None
        self.persist_snap_ids = set()
        self.snapshot_persist_number = snapshot_persist_number
        self.persist_logger = None
        # init persist log dir
        self.persist_log_dir = Config.get("emergency.emergency_log_base_dir")
        os.makedirs(self.persist_log_dir, exist_ok=True)
        # used by cpu full and io full
        self.sorted_top_sql_dict = None
        self.overtime_sess_list = []
        # used by sessions full and connections full
        self.whitelist = None
        self.execute_query = util.get_execute_query(self.logger)
        self.execute_noreturn_query = util.get_execute_noreturn_query(self.logger)

    @abstractmethod
    def analyze(self):
        pass

    @abstractmethod
    def handle_emergency_command(self, stdscr, command, value):
        pass

    def append_split_string(self, text, prefix="CMD"):
        width = 140
        for i in range(0, len(prefix) + len(text), width):
            if i == 0:
                self.emergency_info.append(f"{prefix}: {text[i:i + width]}")
            else:
                self.emergency_info.append(f"{text[i:i + width]}")

    def persist_to_log(self, dump_data_array):
        for dump_data in dump_data_array:
            if len(dump_data) == 0:
                continue

            max_y = max(dump_data.keys()) if dump_data else 0
            max_x = max(max(row.keys()) for row in dump_data.values()) if max_y >= 0 else 0

            lines = []
            for y in range(max_y + 1):
                line = []
                if y in dump_data:
                    for x in range(max_x + 1):
                        char = dump_data[y].get(x, ' ')
                        line.append(char)

                lines.append(''.join(line))

            self.persist_logger.info('\n'.join(lines))
            self.persist_logger.info('')

        # split line
        self.persist_logger.info('=' * 150)
        self.persist_logger.info('')

    def persist(self, dump_data_dict):
        if self.emergency_triggered:
            if not self.need_persist:  # first trigger
                self.need_persist = True
                self.start_persist_time = datetime.now().strftime("%Y%m%d%H%M%S")
                log_name = f"dbtop_emergency_{self.name}_{self.start_persist_time}"
                log_file_name = os.path.join(self.persist_log_dir, f"dbtop_emergency_{self.name}_{self.start_persist_time}.log")
                self.persist_logger = log.Logger(name=log_name,
                                                 log_file=log_file_name,
                                                 level='INFO',
                                                 fmt="%(message)s",
                                                 max_bytes=(Config.get("emergency.log_file_max_size") * 1024 * 1024))
                self.logger.info("Emergency triggered: module = %s, start_persist_snap_id = %d, snapshot_persist_num = %d",
                                 self.name, self.start_persist_snap_id, self.snapshot_persist_number)

        if self.need_persist:
            self.logger.debug("Emergency persist: module = %s, snapshot_persist_num = %d, curr_persist_num = %d",
                             self.name, self.snapshot_persist_number, len(self.persist_snap_ids))
            if len(self.persist_snap_ids) >= self.snapshot_persist_number:
                self.logger.info("Emergency recovered: module = %s, stop persist dbtop info", self.name)
                # rename log file
                end_persist_time = datetime.now().strftime("%Y%m%d%H%M%S")
                old_name = os.path.join(self.persist_log_dir, f"dbtop_emergency_{self.name}_{self.start_persist_time}.log")
                new_name = os.path.join(self.persist_log_dir, f"dbtop_emergency_{self.name}_{self.start_persist_time}_{end_persist_time}.log")

                try:
                    os.rename(old_name, new_name)
                except FileNotFoundError:
                    self.logger.error(f"Rename failed: {old_name} not exists.")
                except FileExistsError:
                    self.logger.error(f"Rename failed: {new_name} already exists.")

                # reset
                self.need_persist = False
                self.start_persist_snap_id = 0
                self.start_persist_time = None
                self.persist_snap_ids.clear()
                self.persist_logger = None
                return

            for snap_id in sorted(dump_data_dict.keys()):
                if snap_id < self.start_persist_snap_id:
                    continue

                if snap_id in self.persist_snap_ids:
                    continue

                self.persist_snap_ids.add(snap_id)

                dump_data_array = dump_data_dict[snap_id]
                self.persist_to_log(dump_data_array)

    def analyze_session(self, target_ste, overtime_thresh):
        # reset old value
        self.sorted_top_sql_dict = None
        self.overtime_sess_list = []

        session_time_result = self.execute_query(SESSION_TIME_SQL)
        if session_time_result is None:
            self.logger.error("Exec query failed.")
            return

        session_time_dict = dict()
        for row in session_time_result:
            sid = str(row[0])
            if sid not in session_time_dict:
                session_time_dict[sid] = []
            session_time_dict[sid].append([row[1], row[2]])

        # key: sql_id  value: [ active_sess_num, DB_TIME, CPU_TIME, DATA_IO_TIME, analyze cmd, query ]
        top_sql_dict = dict()
        for session_row in self.full_session:
            sid = session_row[0]
            sql_id = session_row[4]
            query = session_row[5]
            state = session_row[9]
            session_ste = session_row[10]
            sql_exec_start = session_row[13]
            xact_run_time = session_row[14]

            if not (state == 'ACTIVE' and sql_exec_start is not None):
                continue
            if sql_id is None or sql_id == '' or sql_id == 0:
                continue
            if session_ste != target_ste:
                continue
            # only those exceeding the threshold are included in the statistics
            xact_run_time_ms = round(xact_run_time / 1000, 2) if xact_run_time else 0  # "us" -> "ms"
            if xact_run_time_ms <= overtime_thresh:
                self.logger.debug("xact runtime too short, xact_run_time: %d, overtime_thresh: %d", xact_run_time_ms, overtime_thresh)
                continue
            self.overtime_sess_list.append(str(sid))
            self.emergency_pids.append(sid)
            if top_sql_dict.get(sql_id) is None:
                top_sql_dict[sql_id] = [0] * 6

                # make analyze command
                pattern = r'(?:FROM|UPDATE|INSERT\s+INTO|DELETE\s+FROM)\s+([\w\.]+)'
                match = re.search(pattern, query, re.IGNORECASE) if query else None
                if match:
                    table_name = match.group(1)
                    self.logger.debug("table name: %s", table_name)
                    analyze_command = f'EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname=>NULL, tabname=>\'{table_name}\');'
                else:
                    self.logger.error("unable to extract table name, sql: %s", query)
                    analyze_command = 'None'

                if query is not None:
                    query = query.split('\n')[0]

                top_sql_dict[sql_id][4] = analyze_command
                top_sql_dict[sql_id][5] = query

            top_sql_dict[sql_id][0] += 1
            session_time = session_time_dict.get(str(sid))
            if session_time is not None:
                for session_time_item in session_time:
                    stat_name = session_time_item[0]
                    if stat_name == 'DB time':
                        top_sql_dict[sql_id][1] += session_time_item[1]
                    elif stat_name == 'DB CPU':
                        top_sql_dict[sql_id][2] += session_time_item[1]
                    elif stat_name in ('db file sequential read time', 'db file scattered read time'):
                        top_sql_dict[sql_id][3] += session_time_item[1]

        self.sorted_top_sql_dict = dict(sorted(top_sql_dict.items(), key=lambda item: item[1][0], reverse=True))

    def terminate_session(self, sid, serial_num):
        command = f"ALTER SYSTEM KILL SESSION '{sid},{serial_num}' IMMEDIATE"
        self.logger.warning("Exec command: %s", command)
        self.execute_noreturn_query(command)

    def terminate_limited_sessions(self, emergency_sql_id, max_terminate_count):
        anonymous_block = constants.TERMINATE_LIMITED_SESSIONS_ANONYMOUS_BLOCK.format(
            sql_id=emergency_sql_id, max_terminate_count=max_terminate_count)
        self.logger.warning("Exec anonymous: %s", anonymous_block)
        self.execute_noreturn_query(anonymous_block)

    def terminate_limited_sessions_withtime(self, emergency_sql_id, max_terminate_count, timeout_thresh):
        # Convert ms threshold to seconds for Oracle
        timeout_thresh_secs = round(timeout_thresh / 1000, 2)
        anonymous_block = constants.TERMINATE_LIMITED_SESSIONS_WITHTIME_ANONYMOUS_BLOCK.format(
            sql_id=emergency_sql_id, max_terminate_count=max_terminate_count, timeout_thresh_secs=timeout_thresh_secs)
        self.logger.warning("Exec anonymous: %s", anonymous_block)
        self.execute_noreturn_query(anonymous_block)

    def terminate_unlimited_sessions_withtime(self, emergency_sql_id, timeout_thresh):
        timeout_thresh_secs = round(timeout_thresh / 1000, 2)
        anonymous_block = constants.TERMINATE_UNLIMITED_SESSIONS_WITHTIME_ANONYMOUS_BLOCK.format(
            sql_id=emergency_sql_id, timeout_thresh_secs=timeout_thresh_secs)
        self.logger.warning("Exec anonymous: %s", anonymous_block)
        self.execute_noreturn_query(anonymous_block)

    def terminate_idle_sessions(self):
        """Terminate INACTIVE sessions (Oracle equivalent of idle)"""
        whitelist_clause = ""
        if self.whitelist:
            users_str = ','.join(f"'{u}'" for u in self.whitelist)
            whitelist_clause = f" AND s.USERNAME NOT IN ({users_str})"

        plsql = f"""
DECLARE
    v_count NUMBER := 0;
BEGIN
    FOR rec IN (
        SELECT s.SID, s.SERIAL#
        FROM v$session s
        WHERE s.TYPE = 'USER'
          AND s.STATUS = 'INACTIVE'
          AND s.WAIT_CLASS = 'Idle'
          AND s.TADDR IS NULL{whitelist_clause}
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';
            v_count := v_count + 1;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;"""
        self.execute_noreturn_query(plsql)

    def terminate_idle_in_xact_sessions(self):
        """Terminate sessions that are INACTIVE but have open transactions"""
        whitelist_clause = ""
        if self.whitelist:
            users_str = ','.join(f"'{u}'" for u in self.whitelist)
            whitelist_clause = f" AND s.USERNAME NOT IN ({users_str})"

        plsql = f"""
DECLARE
    v_count NUMBER := 0;
BEGIN
    FOR rec IN (
        SELECT s.SID, s.SERIAL#
        FROM v$session s
        JOIN v$transaction t ON s.TADDR = t.ADDR
        WHERE s.TYPE = 'USER'
          AND s.STATUS = 'INACTIVE'{whitelist_clause}
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';
            v_count := v_count + 1;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;"""
        self.execute_noreturn_query(plsql)

    def terminate_none_sessions(self):
        """Terminate sessions with no active transaction and no SQL running"""
        whitelist_clause = ""
        if self.whitelist:
            users_str = ','.join(f"'{u}'" for u in self.whitelist)
            whitelist_clause = f" AND s.USERNAME NOT IN ({users_str})"

        plsql = f"""
DECLARE
    v_count NUMBER := 0;
BEGIN
    FOR rec IN (
        SELECT s.SID, s.SERIAL#
        FROM v$session s
        WHERE s.TYPE = 'USER'
          AND s.STATUS = 'INACTIVE'
          AND s.SQL_ID IS NULL
          AND s.TADDR IS NULL{whitelist_clause}
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';
            v_count := v_count + 1;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;"""
        self.execute_noreturn_query(plsql)
