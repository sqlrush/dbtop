# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
性能抖动检测模块 (Performance Jitter)

检测数据库性能指标的突变，识别性能抖动事件。

检测逻辑:
    - 监控 db%、TPS、QPS 等关键指标的变化幅度
    - 当指标在短时间内出现大幅波动时触发应急
    - 结合等待事件分析抖动根因

告警输出:
    - 记录抖动时刻的指标值和变化幅度
    - 输出可能的根因分析（等待事件、资源瓶颈等）
"""

from .emergency_base import Emergency
from common.config import Config
from common import alarm


MODULE_NAME = 'PerformanceJitter'
MODULE_HEADER = "[EMER02 - PerformanceJitter]"

class PerformanceJitter(Emergency):
    def __init__(self, logger, db_persist):
        super().__init__(MODULE_NAME, MODULE_HEADER, logger, db_persist, Config.get("emergency.performance_jitter.snapshot_persist_number"))
        self.data_dict = {}
        self.first = True

    def analyze(self):
        if self.first:
            self.first = False
            return

        if len(self.data_dict) == Config.get("emergency.performance_jitter.snapshot_compare_scope"):
            min_key = min(self.data_dict.keys())
            del self.data_dict[min_key]

        curr_data = {}
        ins_asc_num = 0
        for session_row in self.full_session:
            sess_state = session_row[9]
            if sess_state == 'ACTIVE':
                ins_asc_num += 1

        curr_data["snap_ts"] = self.curr_snap_ts
        curr_data["asc"] = ins_asc_num
        curr_data["cpu"] = self.curr_os[1]
        curr_data["r/s"] = self.curr_os[3]
        curr_data["w/s"] = self.curr_os[4]
        curr_data["r_await"] = self.curr_os[7]
        curr_data["w_await"] = self.curr_os[8]
        curr_data["aqu_sz"] = self.curr_os[11]

        self.data_dict[self.curr_snap_id] = curr_data

        for last_snap_id in sorted(self.data_dict.keys()):
            if last_snap_id == self.curr_snap_id:
                break

            last_data = self.data_dict[last_snap_id]
            last_ins_asc = last_data["asc"]
            last_cpu = last_data["cpu"]
            last_rps = last_data["r/s"]
            last_wps = last_data["w/s"]
            last_r_await = last_data["r_await"]
            last_w_await = last_data["w_await"]
            last_aqu_sz = last_data["aqu_sz"]

            if curr_data["asc"] > (last_ins_asc * Config.get("emergency.performance_jitter.ins_acs_pct_thresh")) and \
                (curr_data["asc"] - last_ins_asc) > Config.get("emergency.performance_jitter.ins_acs_abs_thresh"):
                self.trigger_emergency("asc", last_snap_id, self.curr_snap_id)
                break

            if last_cpu != 0:
                if curr_data["cpu"] > (last_cpu * Config.get("emergency.performance_jitter.os_cpu_pct_thresh")) and \
                    curr_data["cpu"] > Config.get("emergency.performance_jitter.os_cpu_thresh"):
                    self.trigger_emergency("cpu", last_snap_id, self.curr_snap_id)
                    break

            if curr_data["r/s"] != 0 and last_rps != 0:
                if curr_data["r_await"] > (last_r_await * Config.get("emergency.performance_jitter.rw_await_pct_thresh")) and \
                     (curr_data["r_await"] - last_r_await) > Config.get("emergency.performance_jitter.rw_await_abs_thresh"):
                    self.trigger_emergency("r_await", last_snap_id, self.curr_snap_id)
                    break

            if curr_data["w/s"] != 0 and last_wps != 0:
                if curr_data["w_await"] > (last_w_await * Config.get("emergency.performance_jitter.rw_await_pct_thresh")) and \
                    (curr_data["w_await"] - last_w_await) > Config.get("emergency.performance_jitter.rw_await_abs_thresh"):
                    self.trigger_emergency("w_await", last_snap_id, self.curr_snap_id)
                    break

            if curr_data["aqu_sz"] > (last_aqu_sz * Config.get("emergency.performance_jitter.aqu_sz_pct_thresh")) and \
                (curr_data["aqu_sz"] - last_aqu_sz) > Config.get("emergency.performance_jitter.aqu_sz_abs_thresh"):
                self.trigger_emergency("aqu_sz", last_snap_id, self.curr_snap_id)
                break

    def trigger_emergency(self, item, last_snap_id, curr_snap_id):
        self.emergency_triggered = True
        self.start_persist_snap_id = last_snap_id

        last_data = self.data_dict[last_snap_id]
        last_snap_ts = last_data["snap_ts"]
        last_ins_asc = last_data["asc"]
        last_cpu = last_data["cpu"]
        last_r_await = last_data["r_await"]
        last_w_await = last_data["w_await"]
        last_aqu_sz = last_data["aqu_sz"]

        curr_data = self.data_dict[curr_snap_id]
        curr_snap_ts = curr_data["snap_ts"]
        curr_ins_asc = curr_data["asc"]
        curr_cpu = curr_data["cpu"]
        curr_r_await = curr_data["r_await"]
        curr_w_await = curr_data["w_await"]
        curr_aqu_sz = curr_data["aqu_sz"]

        self.emergency_info.append(
            f"Snapshot:  ID       TIMESTAMP            INS_ACS  CPU%     R_AWAIT  W_AWAIT  AQU_SZ")
        self.emergency_info.append(
            f"  Prev ->  {last_snap_id:<7}  {last_snap_ts:<}  {last_ins_asc:<7}  {last_cpu:<5}    {last_r_await:<7}  {last_w_await:<7}  {last_aqu_sz:<3}")
        self.emergency_info.append(
            f"  Curr ->  {curr_snap_id:<7}  {curr_snap_ts:<}  {curr_ins_asc:<7}  {curr_cpu:<5}    {curr_r_await:<7}  {curr_w_await:<7}  {curr_aqu_sz:<3}")

        key = f"{MODULE_NAME}_{item}"
        value = f"DBTOP检测到性能抖动，发生抖动的指标为：{item}，抖动前的值：{last_data[item]}，抖动前的时间：{last_snap_ts}，抖动后的值：{curr_data[item]}"
        alarm.check_and_report_alarm(self.logger, key, value, True)

    def handle_emergency_command(self, stdscr, command, value):
        pass
