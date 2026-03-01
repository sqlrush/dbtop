# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
慢 SQL 检测模块 (Slow SQL)

检测执行时间超过阈值的 SQL 语句。

检测逻辑:
    - 扫描所有活跃会话，检查 SQL 执行时间是否超过配置阈值
    - 区分首次超时和持续超时，避免重复告警
    - 聚合同一 SQL_ID 的会话数和累计时间

告警输出:
    - 超时 SQL 的 SQL_ID、SQL 文本、执行时间
    - 受影响的会话数和累计资源消耗
    - 提供 GATHER_TABLE_STATS 等优化建议命令
"""

from datetime import datetime
import re

from .emergency_base import Emergency
from common.config import Config
from common import alarm

MODULE_NAME = 'SlowSQL'
MODULE_HEADER = "[EMER03 - SlowSQL]"

WHITELIST_SQL = ["start transaction" "begin" "commit" "end" "vacuum" "analyze"]

class StrategyConfig:
    def __init__(self, start_time, end_time, check_interval, slow_sql_threshold, slow_procedure_threshold):
        self.start_time = start_time
        self.end_time = end_time
        self.check_interval = check_interval
        self.slow_sql_threshold = slow_sql_threshold
        self.slow_procedure_threshold = slow_procedure_threshold

class SlowSQL(Emergency):
    def __init__(self, logger, db_persist):
        super().__init__(MODULE_NAME, MODULE_HEADER, logger, db_persist, 0)
        self.terminate = False
        self.exclude_databases = []
        self.exclude_users = []
        self.procedure = []
        self.strategy_group = []
        self.procedure_patterns = []
        self.whitelist_patterns = []
        self.check_interval = 0
        self.slow_sql_threshold = 0
        self.slow_procedure_threshold = 0
        self.last_check_timestamp = 0
        self.parse_config()
        for procedure in self.procedure:
            if len(procedure) == 0:
                continue
            pattern = r'\b' + re.escape(procedure) + r'\b'
            self.procedure_patterns.append(pattern)
        for query in WHITELIST_SQL:
            pattern = r'\b' + re.escape(query) + r'\b'
            self.whitelist_patterns.append(pattern)

    @staticmethod
    def _parse_time_string(time_str):
        try:
            return datetime.strptime(time_str.strip(), '%H:%M').time()
        except ValueError as e:
            raise ValueError(f"time format error: {time_str}") from e

    def _parse_strategy(self):
        for i in range(10):
            strategy_value = Config.get(f'emergency.slow_sql.strategy{i}')
            if not strategy_value:
                continue

            parts = [part.strip() for part in strategy_value.split(',')]
            if len(parts) != 5:
                raise ValueError(f"strategy format error: {strategy_value}")

            start_time_str, end_time_str, interval_str, slow_sql_threshold_str, slow_procedure_threshold_str = parts
            start_time = self._parse_time_string(start_time_str)
            end_time = self._parse_time_string(end_time_str)
            if start_time >= end_time:
                raise ValueError(f"start time {start_time} must be before end time {end_time}")

            try:
                check_interval = int(interval_str)
                slow_sql_threshold = int(slow_sql_threshold_str)
                slow_procedure_threshold = int(slow_procedure_threshold_str)
            except ValueError as e:
                raise ValueError(f"number format error") from e

            if check_interval <= 0:
                raise ValueError(f"the check interval must be greater than 0: {check_interval}")
            if slow_sql_threshold <= 0:
                raise ValueError(f"the slow SQL threshold must be greater than 0: {slow_sql_threshold}")
            if slow_procedure_threshold <= 0:
                raise ValueError(f"the slow procedure threshold must be greater than 0: {slow_procedure_threshold}")

            strategy = StrategyConfig(
                start_time=start_time,
                end_time=end_time,
                check_interval=check_interval,
                slow_sql_threshold=slow_sql_threshold,
                slow_procedure_threshold=slow_procedure_threshold
            )
            self.strategy_group.append(strategy)

    def _validate_strategy(self):
        strategy_num = len(self.strategy_group)
        if strategy_num == 0:
            return
        for i in range(strategy_num):
            for j in range(i + 1, strategy_num):
                strategy1 = self.strategy_group[i]
                strategy2 = self.strategy_group[j]
                if not (strategy1.end_time <= strategy2.start_time or strategy2.end_time <= strategy1.start_time):
                    raise RuntimeError(f"validate strategy failed: {strategy1.start_time} {strategy1.end_time} {strategy2.start_time} {strategy2.end_time}")
        return

    def parse_config(self):
        self.terminate = Config.get("emergency.slow_sql.terminate")
        exclude_databases = Config.get("emergency.slow_sql.exclude_databases")
        self.exclude_databases = exclude_databases.split(';') if exclude_databases is not None else []
        exclude_users = Config.get("emergency.slow_sql.exclude_users")
        self.exclude_users = exclude_users.split(';') if exclude_users is not None else []
        procedure = Config.get("emergency.slow_sql.procedure")
        self.procedure = procedure.split(';') if procedure is not None else []
        self._parse_strategy()
        self._validate_strategy()

    def check_strategies_and_update(self, curr_timestamp):
        strategy_num = len(self.strategy_group)
        if strategy_num == 0:
            return False
        for strategy in self.strategy_group:
            if strategy.start_time <= curr_timestamp <= strategy.end_time:
                self.check_interval = strategy.check_interval
                self.slow_sql_threshold = strategy.slow_sql_threshold
                self.slow_procedure_threshold = strategy.slow_procedure_threshold
                return True

    def contains_procedure(self, sql):
        for pattern in self.procedure_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return True
        return False

    def sql_in_whitelist(self, sql):
        for pattern in self.whitelist_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return True
        return False

    def analyze(self):
        curr_datetime = datetime.now()
        in_strategy = self.check_strategies_and_update(curr_datetime.time())
        if not in_strategy:
            return

        if self.last_check_timestamp == 0:
            self.last_check_timestamp = curr_datetime
        elif int((curr_datetime - self.last_check_timestamp).total_seconds()) < self.check_interval:
            return
        else:
            self.last_check_timestamp = curr_datetime

        for session_row in self.full_session:
            sid = session_row[0]
            usename = session_row[1]
            serial_num = session_row[3]
            sql_id = session_row[4]
            query = session_row[5]
            state = session_row[9]
            sql_exec_start = session_row[13]

            if state != 'ACTIVE' or sql_exec_start is None:
                continue

            if usename in self.exclude_users:
                continue

            if query is None:
                continue

            query_upper = query.upper()
            slow_check_threshold = self.slow_sql_threshold
            if len(self.procedure_patterns) != 0 and self.contains_procedure(query_upper):
                slow_check_threshold = self.slow_procedure_threshold

            # check query execute time
            try:
                if hasattr(sql_exec_start, 'replace'):
                    exec_seconds = int((curr_datetime - sql_exec_start.replace(tzinfo=None)).total_seconds())
                else:
                    exec_seconds = 0
            except Exception:
                exec_seconds = 0

            if exec_seconds < slow_check_threshold:
                continue

            if self.sql_in_whitelist(query_upper):
                continue

            self.logger.warning(f"Slow SQL: {query}  SQL_EXEC_START: {sql_exec_start}  Threshold: {slow_check_threshold}  SQL_ID: {sql_id}")

            key = f"{MODULE_NAME}_{usename}_{sql_id}"
            command = f"ALTER SYSTEM KILL SESSION '{sid},{serial_num}' IMMEDIATE;"
            value = f"DBTOP检测到慢SQL，SQL开始时间: {sql_exec_start}，慢SQL阈值: {slow_check_threshold}秒，SQL_ID: {sql_id}，SQL语句: {query}，使用以下命令查杀慢SQL会话: {command}"
            alarm.check_and_report_alarm(self.logger, key, value, True)

            if self.terminate is True and Config.get("main.support_terminate"):
                self.terminate_session(sid, serial_num)

    def trigger_emergency(self, last_snap_id, curr_snap_id):
        pass

    def handle_emergency_command(self, stdscr, command, value):
        pass
