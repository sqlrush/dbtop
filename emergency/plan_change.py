# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
执行计划变更检测模块 (Plan Change)

检测 Oracle SQL 执行计划是否发生变更，变更通常导致性能回退。

检测逻辑:
    - 对比 v$sql 中同一 SQL_ID 的不同 plan_hash_value
    - 当检测到 SQL 的执行计划发生变更时触发应急
    - 将变更的 SQL_ID 高亮标记在会话面板中（黄色）

交互命令:
    - 展示执行计划变更详情
    - 提供 SQL Profile / SQL Plan Baseline 固定执行计划的建议命令
"""

import curses
from datetime import datetime
import re

from common.config import Config
from common import alarm, util
from .emergency_base import Emergency

STATEMENT_QUERY_SQL = """SELECT SQL_ID, SUM(EXECUTIONS), SUM(ELAPSED_TIME), SUM(CPU_TIME)
                    FROM v$sql
                    WHERE SQL_TEXT NOT LIKE '%BEGIN%' AND SQL_TEXT NOT LIKE '%COMMIT%'
                    GROUP BY SQL_ID"""
SQL_ID_IDX = 0
N_CALLS_IDX = 1
DB_TIME_IDX = 2
CPU_TIME_IDX = 3
STATEMENT_COL_NUM = 4

MODULE_NAME = 'PlanChange'
MODULE_HEADER = "[EMER01 - PlanChange] - select the line with 'SQL_ID' and press 'k' to terminate abnormal sessions"

class PlanChange(Emergency):
    def __init__(self, logger, db_persist):
        super().__init__(MODULE_NAME, MODULE_HEADER, logger, db_persist, Config.get("emergency.plan_change.snapshot_persist_number"))
        # save first statement result
        statement_result = self.execute_query(STATEMENT_QUERY_SQL)
        if statement_result is None:
            self.logger.error("Query v$sql failed.")
            return
        self.last_statement_result = statement_result
        self.last_snap_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def analyze(self):
        curr_statement_result = self.execute_query(STATEMENT_QUERY_SQL)
        if curr_statement_result is None:
            self.logger.error("Query v$sql failed.")
            return

        curr_analyze_result = self.analyze_statement(curr_statement_result)
        if curr_analyze_result is None:
            self.last_statement_result = curr_statement_result
            self.last_snap_ts = self.curr_snap_ts
            return

        self.analyze_plan_change(curr_analyze_result)

        self.last_statement_result = curr_statement_result
        self.last_snap_ts = self.curr_snap_ts

    def analyze_statement(self, curr_statement):
        time_diff = (datetime.strptime(self.curr_snap_ts, "%Y-%m-%d %H:%M:%S").timestamp() -
                     datetime.strptime(self.last_snap_ts, "%Y-%m-%d %H:%M:%S").timestamp())
        if time_diff == 0:
            return None

        analyze_result = dict()
        last_statement_dict = dict()
        if self.last_statement_result is not None:
            for row_value in self.last_statement_result:
                sql_id = row_value[SQL_ID_IDX]
                last_statement_dict[sql_id] = row_value

        # build active session dict
        active_session_dict = dict()
        for session_row in self.full_session:
            sql_id = session_row[4]
            sess_state = session_row[9]
            if sess_state == 'ACTIVE':
                if active_session_dict.get(sql_id) is None:
                    active_session_dict[sql_id] = 1
                else:
                    active_session_dict[sql_id] += 1

        curr_cpu = self.curr_os[1]  # see %CPU in operating_system.cfg
        for curr_statement_row in curr_statement:
            sql_id = curr_statement_row[SQL_ID_IDX]

            curr_sql_acs_cnt = active_session_dict.get(sql_id, 0)

            last_statement_row = last_statement_dict.get(sql_id)
            if last_statement_row is None:
                last_statement_row = [0] * STATEMENT_COL_NUM

            n_calls_diff = float(curr_statement_row[N_CALLS_IDX] - last_statement_row[N_CALLS_IDX])
            curr_sql_qps = round(float(n_calls_diff) / time_diff, 1)

            if n_calls_diff > 0:
                curr_sql_latency = round(
                    float(curr_statement_row[DB_TIME_IDX] - last_statement_row[DB_TIME_IDX]) / n_calls_diff, 2)
                curr_sql_cputime = round(
                    float(curr_statement_row[CPU_TIME_IDX] - last_statement_row[CPU_TIME_IDX]) / n_calls_diff, 2)
            else:
                curr_sql_latency = 0
                curr_sql_cputime = 0

            if curr_sql_acs_cnt == 0 and curr_sql_latency == 0:
                continue

            sql_info = {
                "db_id": self.db_id,
                "snap_id": self.curr_snap_id,
                "snap_ts": self.curr_snap_ts,
                "unique_sql_id": sql_id,
                "sql_acs_cnt": curr_sql_acs_cnt,
                "sql_latency": curr_sql_latency,
                "sql_cputime": curr_sql_cputime,
                "sql_qps": curr_sql_qps
            }
            self.db_persist.persist_sql_info(sql_info)
            analyze_result[sql_id] = sql_info

        # calculate ins_acs_cnt
        curr_ins_acs_cnt = 0
        for sql_acs_cnt in active_session_dict.values():
            curr_ins_acs_cnt += sql_acs_cnt

        ins_info = {
            "db_id": self.db_id,
            "snap_id": self.curr_snap_id,
            "snap_ts": self.curr_snap_ts,
            "ins_acs_cnt": curr_ins_acs_cnt,
            "ins_cpu_utl": curr_cpu
        }
        self.db_persist.persist_ins_info(ins_info)

        analyze_result[0] = ins_info
        return analyze_result

    def analyze_plan_change(self, curr_analyze_result):
        curr_ins_info = curr_analyze_result[0]
        if curr_ins_info["ins_cpu_utl"] < Config.get("emergency.plan_change.os_cpu_thresh"):
            self.judge_emergency_sql_recovered(None)
            self.logger.debug("skip simple case, cpu: %d", curr_ins_info["ins_cpu_utl"])
            return

        trigger_emergency_sql_ids = []
        earliest_trigger_emergency_snap_id = 0
        for sql_id, curr_sql_info in curr_analyze_result.items():
            if sql_id == 0:
                continue

            curr_sql_acs_cnt = curr_sql_info["sql_acs_cnt"]
            curr_sql_latency = curr_sql_info["sql_latency"]
            if curr_sql_acs_cnt <= curr_ins_info["ins_acs_cnt"] * Config.get("emergency.plan_change.sql_acs_ins_pct_thresh"):
                continue

            emergency_sql_info_snaps = self.db_persist.get_emergency_sql_info_snap(self.db_id, sql_id)
            if emergency_sql_info_snaps is not None and len(emergency_sql_info_snaps) > 0:
                sql_info_snap = emergency_sql_info_snaps[0]
                last_snap_id = sql_info_snap[1]
                last_sql_acs_cnt = sql_info_snap[4]
                last_sql_latency = sql_info_snap[5]

                if curr_sql_acs_cnt <= max(last_sql_acs_cnt + Config.get("emergency.plan_change.sql_acs_abs_thresh"),
                                           last_sql_acs_cnt * Config.get("emergency.plan_change.sql_acs_pct_thresh")):
                    continue

                if curr_sql_latency != 0 and curr_sql_latency <= last_sql_latency * Config.get("emergency.plan_change.sql_latency_pct_thresh"):
                    continue

                trigger_emergency_sql_ids.append(sql_id)
                if (earliest_trigger_emergency_snap_id == 0 or
                        last_snap_id < earliest_trigger_emergency_snap_id):
                    earliest_trigger_emergency_snap_id = last_snap_id
                self.trigger_emergency(curr_ins_info, curr_sql_info, sql_info_snap, True)
                continue

            sql_info_snaps = self.db_persist.get_sql_info_snap(self.db_id, curr_ins_info["snap_id"], sql_id)
            if sql_info_snaps is None:
                self.logger.warning("Query sql snap return None, sql_id: %s", sql_id)
                continue

            for sql_info_snap in sql_info_snaps:
                db_id = sql_info_snap[0]
                last_snap_id = sql_info_snap[1]
                last_snap_ts = sql_info_snap[2]
                last_sql_acs_cnt = sql_info_snap[4]
                last_sql_latency = sql_info_snap[5]
                last_sql_cputime = sql_info_snap[6]
                last_sql_qps = sql_info_snap[7]

                if curr_sql_acs_cnt <= max(last_sql_acs_cnt + Config.get("emergency.plan_change.sql_acs_abs_thresh"),
                                           last_sql_acs_cnt * Config.get("emergency.plan_change.sql_acs_pct_thresh")):
                    continue

                if curr_sql_latency <= last_sql_latency * Config.get("emergency.plan_change.sql_latency_pct_thresh"):
                    continue

                emergency_sql_info = {
                    "db_id": db_id,
                    "snap_id": last_snap_id,
                    "snap_ts": last_snap_ts,
                    "unique_sql_id": sql_id,
                    "sql_acs_cnt": last_sql_acs_cnt,
                    "sql_latency": last_sql_latency,
                    "sql_cputime": last_sql_cputime,
                    "sql_qps": last_sql_qps,
                    "emergency_ts": curr_ins_info["snap_ts"],
                    "recovered": False
                }
                self.db_persist.persist_emergency_sql_info(emergency_sql_info)

                trigger_emergency_sql_ids.append(sql_id)
                if (earliest_trigger_emergency_snap_id == 0 or
                        last_snap_id < earliest_trigger_emergency_snap_id):
                    earliest_trigger_emergency_snap_id = last_snap_id
                self.trigger_emergency(curr_ins_info, curr_sql_info, sql_info_snap, False)
                break

        self.emergency_sql_ids = trigger_emergency_sql_ids
        if earliest_trigger_emergency_snap_id != 0:
            self.start_persist_snap_id = earliest_trigger_emergency_snap_id
        self.judge_emergency_sql_recovered(trigger_emergency_sql_ids)

    def judge_emergency_sql_recovered(self, trigger_emergency_sql_ids):
        emergency_sql_info_snaps = self.db_persist.get_emergency_sql_unrecovered(self.db_id)
        if emergency_sql_info_snaps is None:
            return

        for emergency_sql_info_snap in emergency_sql_info_snaps:
            snap_id = emergency_sql_info_snap[1]
            sql_id = emergency_sql_info_snap[3]
            emergency_ts = emergency_sql_info_snap[8]
            if type(emergency_ts) == str:
                time_diff = (datetime.strptime(self.last_snap_ts, "%Y-%m-%d %H:%M:%S").timestamp() -
                             datetime.strptime(emergency_ts, "%Y-%m-%d %H:%M:%S").timestamp())
            else:
                time_diff = datetime.strptime(self.last_snap_ts, "%Y-%m-%d %H:%M:%S").timestamp() - emergency_ts.timestamp()

            if time_diff < Config.get("emergency.plan_change.observation_time"):
                continue

            if trigger_emergency_sql_ids is None or (
                    trigger_emergency_sql_ids is not None and sql_id not in trigger_emergency_sql_ids):
                self.db_persist.update_emergency_sql_recovered(self.db_id, snap_id, sql_id)
                emergency_ts_str = emergency_ts if type(emergency_ts) == str else emergency_ts.strftime("%Y-%m-%d %H:%M:%S")
                self.logger.info("Plan change recovered: db_id = %d, snap_id = %d, sql_id = %s, emergency_ts = %s",
                                 self.db_id, snap_id, sql_id, emergency_ts_str)

    def trigger_emergency(self, ins_info, sql_info, sql_info_snap, has_triggered):
        self.emergency_triggered = True

        db_id = sql_info_snap[0]
        snap_id = sql_info_snap[1]
        snap_ts_record = sql_info_snap[2]
        snap_ts = snap_ts_record if type(snap_ts_record) == str else snap_ts_record.strftime("%Y-%m-%d %H:%M:%S")
        sql_id = sql_info_snap[3]
        sql_acs_cnt = sql_info_snap[4]
        sql_latency = sql_info_snap[5]
        sql_cputime = sql_info_snap[6]
        sql_qps = sql_info_snap[7]

        ins_info_snap = self.db_persist.get_ins_info_snap(db_id, snap_id)
        if ins_info_snap is None:
            self.logger.error("Query ins snap failed, db_id: %d, snap_id: %d", db_id, snap_id)
            return
        ins_acs_cnt = ins_info_snap[0][3]
        ins_cpu_utl = ins_info_snap[0][4]

        sql_text = None
        for session in self.full_session:
            sess_sql_id = session[4]
            if sess_sql_id == sql_id:
                sql_text = session[5]
                break

        analyze_command = None
        if sql_text:
            pattern = r'(?:FROM|UPDATE|INSERT\s+INTO|DELETE\s+FROM)\s+([\w\.]+)'
            match = re.search(pattern, sql_text, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                analyze_command = f"EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname=>NULL, tabname=>'{table_name}');"

        if sql_text is not None:
            sql_text = sql_text.split('\n')[0]

        curr_snap_id = ins_info["snap_id"]
        curr_ins_cpu_utl = ins_info["ins_cpu_utl"]
        curr_ins_acs_cnt = ins_info["ins_acs_cnt"]
        curr_sql_acs_cnt = sql_info["sql_acs_cnt"]
        curr_sql_latency = sql_info["sql_latency"]
        curr_sql_cputime = sql_info["sql_cputime"]
        curr_sql_qps = sql_info["sql_qps"]

        sql_acs_pct = round(sql_acs_cnt / ins_acs_cnt, 2) if ins_acs_cnt > 0 else 0
        curr_sql_acs_pct = round(curr_sql_acs_cnt / curr_ins_acs_cnt, 2) if curr_ins_acs_cnt > 0 else 0

        if analyze_command is not None:
            self.emergency_info.append("SQL_ID: %s    ANALYZE_CMD: '%s'    SQL_TEXT: %s" % (sql_id, analyze_command, sql_text))
        else:
            self.emergency_info.append("SQL_ID: %s    SQL_TEXT: %s" % (sql_id, sql_text))
        self.emergency_info.append(
            f"Snapshot:  ID       TIMESTAMP            SQL_ACS  INS_ACS  PCT   LATENCY      SQL_CPU      SQL_QPS   INS_CPU%")
        self.emergency_info.append(
            f"  Prev ->  {snap_id:<7}  {snap_ts:<}  {sql_acs_cnt:<7}  {ins_acs_cnt:<7}  {sql_acs_pct:<4}  {sql_latency:<11}  {sql_cputime:<11}  {sql_qps:<8}  {ins_cpu_utl:<7}")
        self.emergency_info.append(
            f"  Curr ->  {curr_snap_id:<7}  {self.last_snap_ts:<}  {curr_sql_acs_cnt:<7}  {curr_ins_acs_cnt:<7}  {curr_sql_acs_pct:<4}  {curr_sql_latency:<11}  {curr_sql_cputime:<11}  {curr_sql_qps:<8}  {curr_ins_cpu_utl:<7}")

        terminate_command = f"-- ALTER SYSTEM KILL SESSION for SQL_ID = {sql_id}"

        if not Config.get("main.support_terminate"):
            self.emergency_info.append(f"")
            self.append_split_string(terminate_command)

        # report alarm
        key = f"{MODULE_NAME}_{sql_id}"
        value = f"DBTOP检测到执行计划跳变，发生执行计划跳变的SQL ID：{sql_id}，SQL语句：{sql_text}，使用以下命令更新统计数据：{analyze_command}"
        alarm.check_and_report_alarm(self.logger, key, value, True)

    def handle_emergency_command(self, stdscr, command, value):
        if command == ord('k'):
            pattern = r'SQL_ID:\s+(\w+)'
            match = re.search(pattern, value)
            if not match:
                self.logger.warning("unable to extract sql id, text: %s", value)
                return

            emergency_sql_id = match.group(1)
            if emergency_sql_id not in self.emergency_sql_ids:
                return

            curr_pos_y, curr_pos_x = stdscr.getyx()
            save_y = curr_pos_y
            save_x = curr_pos_x
            stdscr.timeout(-1)
            if not util.terminate_confirm_passed(stdscr):
                stdscr.timeout(Config.get("main.interval") * 1000)
                return
            stdscr.addstr("Input the number of max terminate sessions: ", curses.color_pair(5) | curses.A_BOLD)
            kill_number = util.get_input_number(stdscr)
            stdscr.timeout(Config.get("main.interval") * 1000)
            stdscr.move(save_y, save_x)
            if kill_number > 0:
                self.terminate_limited_sessions(emergency_sql_id, kill_number)
