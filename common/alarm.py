# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
告警模块 (Alarm)

负责将监控指标超阈值事件写入告警文件，支持告警抑制。

核心功能:
    - 监控指标阈值检测：当 TPS、QPS、CPU、连接数等指标超过配置阈值时触发告警
    - 告警抑制：同一告警 key 在 suppression_interval 内不重复上报
    - 应急模块告警：锁阻塞、CPU 满载等应急事件的专用告警通道
    - 告警输出：写入配置的 alarm_file 文件，格式 [ALARM][时间][主机名]告警内容

告警流程:
    start_alarm()                           # 初始化，打开告警文件
    check_and_report_alarm(logger, key, v)  # 检测阈值并上报
    stop_alarm()                            # 关闭告警文件
"""

import socket
import time

from datetime import datetime
from .config import Config

_last_report_alarm_times = {}

_alarm_file_handler = None
_hostname = None

def start_alarm():
    alarm_file = Config.get("alarm.alarm_file")
    if alarm_file is None or len(alarm_file) == 0:
        return

    global _alarm_file_handler
    if _alarm_file_handler is None:
        try:
            _alarm_file_handler = open(alarm_file, 'a', encoding='utf-8')
        except Exception as e:
            raise RuntimeError(f"open alarm file failed: {e}")

    global _hostname
    if _hostname is None:
        _hostname = socket.gethostname()

def stop_alarm():
    global _alarm_file_handler
    if _alarm_file_handler is not None:
        _alarm_file_handler.close()

def should_report_alarm(key):
    global _last_report_alarm_times
    current_time = time.time()

    last_report_time = _last_report_alarm_times.get(key)
    if last_report_time is None or current_time - last_report_time >= Config.get("alarm.suppression_interval"):
        _last_report_alarm_times[key] = current_time
        return True

    return False

def check_and_report_alarm(logger, key, value, emergency=False):
    global _alarm_file_handler
    global _hostname

    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not emergency:
        monitor_item_thresh = Config.get(f"alarm.{key.lower()}")
        if monitor_item_thresh is None or value is None:
            return

        if key == 'CONNECTION(c/m)' or key == 'PROCESSES':
            monitor_value = float(value.split('%')[0])
        else:
            monitor_value = float(value)

        if monitor_value < monitor_item_thresh:
            return

        log_msg = f"[ALARM][{date}][{_hostname}]DBTOP监控指标\"{key}\"超过预先设定的阈值，当前值为：{monitor_value}，阈值为：{monitor_item_thresh}"
    else:
        log_msg = f"[ALARM][{date}][{_hostname}]{value}"

    if not should_report_alarm(key.lower()):
        return

    try:
        _alarm_file_handler.write(log_msg + '\n')
        _alarm_file_handler.flush()
    except Exception as e:
        logger.error(f"write alarm file failed: {e}")
        return
