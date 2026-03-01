# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

import curses
import subprocess
import sys
import threading
import time
import traceback
try:
    import oracledb
except ImportError:
    import cx_Oracle as oracledb


from contextlib import contextmanager
from functools import wraps
from .config import Config

_global_connection = None
_global_connection_lock = threading.Lock()
_last_connect_time = None

_last_refresh_times = {}
_last_cpu_usage = 0
_last_refresh_time = 0

def get_connection():
    global _global_connection
    global _global_connection_lock
    global _last_connect_time

    with _global_connection_lock:
        if _global_connection is not None:
            return _global_connection

        current_time = time.time()
        if _last_connect_time is not None and (current_time - _last_connect_time) <= 1:
            return None

        _last_connect_time = current_time

        if Config.get("main.sysdba"):
            # Local SYSDBA connection (oracle OS user, no password needed)
            connection = oracledb.connect(mode=oracledb.SYSDBA)
        elif not Config.get("main.password_free"):
            dsn = oracledb.makedsn(
                host=Config.get("main.host"),
                port=Config.get("main.port"),
                service_name=Config.get("main.service_name")
            )
            connection = oracledb.connect(
                user=Config.get("main.user"),
                password=Config.get("main.db_password"),
                dsn=dsn
            )
        else:
            dsn = oracledb.makedsn(
                host=Config.get("main.host"),
                port=Config.get("main.port"),
                service_name=Config.get("main.service_name")
            )
            connection = oracledb.connect(
                user=Config.get("main.user"),
                password=Config.get("main.db_password"),
                dsn=dsn
            )

        if connection is not None:
            connection.autocommit = True
            _global_connection = connection
        return _global_connection

def check_connection():
    global _global_connection
    global _global_connection_lock

    try:
        cursor = _global_connection.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        cursor.close()
        return

    except Exception as e:
        with _global_connection_lock:
            try:
                _global_connection.close()
            except Exception:
                pass
            _global_connection = None
        return

def update_cpu_usage(cpu_usage):
    global _last_cpu_usage
    _last_cpu_usage = cpu_usage

def should_refresh_memory(key):
    global _last_cpu_usage
    if _last_cpu_usage >= Config.get("main.dynamic_mem_cpu_thresh"):
        return False

    global _last_refresh_times
    current_time = time.time()

    last_refresh_time = _last_refresh_times.get(key)
    if last_refresh_time is None or current_time - last_refresh_time >= Config.get("main.dynamic_mem_interval"):
        _last_refresh_times[key] = current_time
        return True

    return False

def update_refresh_time():
    global _last_refresh_time
    _last_refresh_time = time.time()

def should_exit():
    global _last_refresh_time
    current_time = time.time()

    if _last_refresh_time != 0 and current_time - _last_refresh_time >= 300:
        return True

    return False

def get_input_number(stdscr):
    input_str = ""
    while True:
        char_code = stdscr.getch()
        if char_code in [10, 13]:
            break
        elif char_code in [127, 8]:
            if input_str:
                input_str = input_str[:-1]
                stdscr.addstr('\b \b')
        elif 48 <= char_code <= 57:
            char_input = chr(char_code)
            input_str += char_input
            stdscr.addstr(char_input)
        stdscr.refresh()
    try:
        return int(input_str) if input_str else 0
    except ValueError:
        return 0

def terminate_confirm_passed(stdscr):
    def safe_chr(value):
        if value < 0:
            return "•"

        if 0 <= value <= 0x10FFFF:
            try:
                return chr(value)
            except (ValueError, TypeError):
                return "?"
        else:
            return "?"

    cursor_y, cursor_x = stdscr.getyx()

    confirm_str = "Confirm again whether you need to execute the terminate command (y/n): "
    stdscr.addstr(confirm_str, curses.color_pair(5) | curses.A_BOLD)

    result = False
    while True:
        input_char = stdscr.getch()
        curses.flushinp()

        if input_char in [10, 13]:
            stdscr.move(cursor_y + 1, 0)
            return result
        elif input_char == ord('y'):
            result = True
        else:
            result = False

        char_display = safe_chr(input_char)
        stdscr.addstr(cursor_y, len(confirm_str), char_display, curses.color_pair(5) | curses.A_BOLD)
        stdscr.refresh()

@contextmanager
def time_statistics_context(logger, name):
    start_time = time.time()
    yield
    end_time = time.time()
    elapsed_time = end_time - start_time
    if elapsed_time > Config.get("main.refresh_analyze_time_thresh"):
        logger.warning(f"Module: '{name}' executed finished in {elapsed_time:.4f} seconds")

def refresh_analyze_wrapper(logger, name, func):
    with time_statistics_context(logger, name):
        try:
            func()
        except Exception:
            logger.error("Exception Traceback:\n%s", traceback.format_exc())

def log_slow_function(logger):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            if elapsed_time > Config.get("main.sql_command_time_thresh"):
                logger.warning(f"Slow func: {func.__name__}  Time used: {elapsed_time:.3f}s  Args: args={args}, kwargs={kwargs}")
            return result
        return wrapper
    return decorator

def create_execute_query(logger):
    @log_slow_function(logger)
    def execute_query(query):
        connection = get_connection()
        if connection is None:
            logger.warning("Connection is None when exec query: %s", query)
            return None

        try:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result

        except Exception as e:
            logger.error(f"Exec query '{query}' failed: {e}")
            check_connection()
            return None

    return execute_query

def get_execute_query(logger):
    return create_execute_query(logger)

def create_execute_noreturn_query(logger):
    @log_slow_function(logger)
    def execute_noreturn_query(query):
        connection = get_connection()
        if connection is None:
            logger.warning("Connection is None when exec query: %s", query)
            return None

        try:
            cursor = connection.cursor()
            cursor.execute(query)
            cursor.close()
            return True

        except Exception as e:
            logger.error(f"Exec query '{query}' failed: {e}")
            check_connection()
            return False

    return execute_noreturn_query

def get_execute_noreturn_query(logger):
    return create_execute_noreturn_query(logger)

def create_execute_os_command(logger):
    @log_slow_function(logger)
    def execute_os_command(command, checking=True):
        kwargs = {
            'shell': True,
            'check': checking,
            'stdout': subprocess.PIPE,
            'stderr': subprocess.PIPE
        }
        if sys.version_info >= (3, 7):
            kwargs['text'] = True
        else:
            kwargs['universal_newlines'] = True
        try:
            process = subprocess.run(command, **kwargs)
            if process.stderr:
                logger.error(f"Exec command '{command}' meets error: {process.stderr}")
                return None
            return process.stdout.strip()
        except Exception as e:
            logger.error(f"Exec command '{command}' failed: {e}")
            return None

    return execute_os_command

def get_execute_os_command(logger):
    return create_execute_os_command(logger)

def build_resource_manager_cmd(sql_id):
    command = f"-- Use Oracle Resource Manager to throttle SQL_ID: {sql_id}"
    return command

def build_sql_quarantine_cmd(sql_id):
    command = f"-- EXEC DBMS_SQLQ.CREATE_QUARANTINE_BY_SQL_ID(SQL_ID => '{sql_id}');"
    return command
