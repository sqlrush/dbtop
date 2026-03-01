# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

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
