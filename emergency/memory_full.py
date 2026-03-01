# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from .emergency_base import Emergency
from common.config import Config
from common import alarm
from monitor.memory import EMER_NULL, EMER_SGA_MEMORY_FULL, EMER_SESSION_PGA_MEMORY_FULL


MODULE_NAME = 'MemoryFull'
MODULE_HEADER = "[EMER03 - MemoryFull] - switch to memory monitor panel to analyze"

class MemoryFull(Emergency):
    def __init__(self, logger, db_persist):
        super().__init__(MODULE_NAME, MODULE_HEADER, logger, db_persist, Config.get("emergency.memory_full.snapshot_persist_number"))
        self.memory_full_type = EMER_NULL

    def analyze(self):
        # reset
        self.memory_full_type = EMER_NULL

        panel_0 = self.curr_memory[0]
        panel_1 = self.curr_memory[1]

        # Panel 0 layout: [DATE, SGA_MAX, SGA_USED%, SGA_FREE, SGA_FREE%, PGA_ALLOC, PGA_USED, PGA_FREE, PGA_FREE%]
        sga_max = panel_0["value"][0][1]       # SGA_MAX (MB)
        sga_used_pct = panel_0["value"][0][2]  # SGA_USED%
        sga_free_pct = panel_0["value"][0][4]  # SGA_FREE%
        pga_alloc = panel_0["value"][0][5]     # PGA_ALLOC (MB)
        pga_used = panel_0["value"][0][6]      # PGA_USED (MB)

        # Check SGA: if SGA free percentage is below threshold
        sga_free_thresh = Config.get("emergency.memory_full.sga_free_pct_thresh")
        if sga_free_pct < sga_free_thresh:
            # Also check panel 1 (SGA components) for top component
            dynamic_header_row = panel_1["header"]
            dynamic_total_row = panel_1["value"][0]
            for idx, value in enumerate(dynamic_header_row):
                if value != "SUM":
                    continue

                sga_total = dynamic_total_row[idx]
                alarm_value = (f"DBTOP检测到SGA内存满，当前SGA总大小{sga_max}MB，"
                               f"SGA使用率{sga_used_pct}%，SGA空闲比例{sga_free_pct}%，"
                               f"SGA组件总占用{sga_total}MB，阈值为{sga_free_thresh}%")
                self.trigger_emergency(EMER_SGA_MEMORY_FULL, alarm_value)
                return

        # Check PGA: if PGA used percentage exceeds threshold
        pga_thresh = Config.get("emergency.memory_full.pga_pct_thresh")
        if pga_alloc > 0:
            pga_used_pct = round(pga_used / pga_alloc * 100, 2)
            if pga_used_pct > pga_thresh:
                alarm_value = (f"DBTOP检测到PGA内存满（会话和进程），"
                               f"当前PGA分配{pga_alloc}MB，PGA使用{pga_used}MB，"
                               f"使用率{pga_used_pct}%，阈值为{pga_thresh}%")
                self.trigger_emergency(EMER_SESSION_PGA_MEMORY_FULL, alarm_value)

    def trigger_emergency(self, memory_full_type, alarm_value):
        self.emergency_triggered = True
        self.memory_full_type = memory_full_type

        panel_2 = self.curr_memory[2]
        panel_3 = self.curr_memory[3]

        session_sid_list = []
        session_memory = "TOP3 SESSION MEMORY (SID PGA_ALLOC):"
        value = panel_2["value"]
        for i, monitor_value_per_line in enumerate(value):
            if i < 3:
                sid = monitor_value_per_line[0]
                pga_mb = monitor_value_per_line[3]  # PGA_ALLOC(MB)
                session_sid_list.append(sid)
                session_memory = f"{session_memory}  [{i+1}] SID:{sid} {pga_mb}MB"
            else:
                break

        process_pid_list = []
        process_memory = "TOP3 PROCESS MEMORY (PID PGA_ALLOC):"
        value = panel_3["value"]
        for i, monitor_value_per_line in enumerate(value):
            if i < 3:
                pid = monitor_value_per_line[0]
                pga_mb = monitor_value_per_line[2]  # PGA_ALLOC(MB)
                process_pid_list.append(pid)
                process_memory = f"{process_memory}  [{i+1}] PID:{pid} {pga_mb}MB"
            else:
                break

        self.emergency_info.append(f"{session_memory}")
        self.emergency_info.append(f"{process_memory}")

        if memory_full_type == EMER_SESSION_PGA_MEMORY_FULL:
            terminate_session_command = None
            if len(session_sid_list) != 0:
                sid_str = ','.join(str(sid) for sid in session_sid_list)
                terminate_session_command = (
                    f"BEGIN\n"
                    f"    FOR rec IN (SELECT SID, SERIAL# FROM v$session WHERE SID IN ({sid_str})) LOOP\n"
                    f"        EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';\n"
                    f"    END LOOP;\n"
                    f"END;")
                alarm_value = (f"{alarm_value}，"
                               f"PGA占用较高的会话SID以及PGA占用大小如下：{session_memory}，"
                               f"使用以下PL/SQL命令快速查杀PGA占用高的会话：{terminate_session_command}")

            # print emergency command
            if terminate_session_command is not None:
                if not Config.get("main.support_terminate"):
                    self.emergency_info.append(f"")
                    self.append_split_string(terminate_session_command, "TERMINATE SESSION")

        # report alarm
        key = f"{MODULE_NAME}_{self.memory_full_type}"
        alarm.check_and_report_alarm(self.logger, key, alarm_value, True)

    def handle_emergency_command(self, stdscr, command, value):
        pass
