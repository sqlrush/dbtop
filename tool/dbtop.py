# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
dbtop 主程序入口 (Main Entry Point)

Oracle 数据库实时监控工具，类似 htop 但面向数据库，基于 curses 终端 UI。

架构概览:
    ┌─────────────────────────────────────────────────┐
    │  DB Monitor    — 版本/角色/SGA/PGA/db%/WTR%     │ ← db.py
    │  OS Monitor    — LOAD/%CPU/%MEM/IO 指标          │ ← operating_system.py
    │  INS Monitor   — SN/AN/TPS/QPS/REDO/连接数       │ ← instance.py
    │  Event Monitor — Top N 等待事件 (RT/Cumulative)   │ ← event.py
    │  Session Monitor — 全量会话列表（可交互）          │ ← session.py
    │  Emergency Panel — 应急预案触发信息               │ ← emergency/
    └─────────────────────────────────────────────────┘

运行模式:
    - 交互模式 (默认): curses 全屏终端 UI，支持快捷键操作
    - Daemon 模式 (-d): 无终端输出，仅写入日志文件，适合后台运行

启动流程:
    1. 解析命令行参数 + 加载 dbtop.cfg 配置文件
    2. 并发用户数检测（防止多实例同时运行）
    3. 数据库连接（SYSDBA 免密 或 用户名/密码）
    4. 初始化 5 个监控模块 + 内存监控 + 应急模块
    5. 启动 DbtopRefresher 后台线程（定时刷新所有监控模块）
    6. 主线程进入事件循环，处理键盘输入和屏幕输出

快捷键:
    r/c — 切换等待事件实时/累计模式
    s — 进入会话交互模式（方向键/排序/终止/详情）
    m — 切换到内存监控视图
    e — 进入应急预案交互模式
    q — 退出

用法:
    dbtop                          # SYSDBA 免密启动
    dbtop -u system -H 10.0.0.1   # 指定用户和主机
    dbtop -d                       # daemon 后台模式
"""

import argparse
import curses
import os
import queue
import sys
import threading
import time
import traceback
import getpass

from monitor import db, instance, event, session, operating_system, memory
from emergency import emergency
from common import alarm, constants, data_logger, log, util
from common.config import Config

MONITOR_WIDTH = 150

CURSOR_Y_START = 12 # start pos_y of session module
CURSOR_X_START = 0


def handle_session_related_keys(stdscr, session_monitor):
    cursor_y, cursor_x = CURSOR_Y_START, CURSOR_X_START
    screen_height, screen_width = stdscr.getmaxyx()
    while True:
        char = stdscr.getch()
        curses.flushinp()
        move_step = min(session_monitor.height - 1, screen_height - CURSOR_Y_START - 1)
        if char == ord('s'):
            pass
        elif char == curses.KEY_UP:
            if cursor_y == CURSOR_Y_START + 1: # already at top of monitor, pageup
                if session_monitor.check_highlight_location(-1, move_step):
                    cursor_y = min(session_monitor.height + CURSOR_Y_START - 1, screen_height - 1)
            else:
                cursor_y -= 1
        elif char == curses.KEY_DOWN:
            if cursor_y == min(CURSOR_Y_START + session_monitor.get_pad_length(), screen_height - 1):  # already at bottom of monitor, pagedown
                if session_monitor.check_highlight_location(1, move_step):
                    cursor_y = CURSOR_Y_START + 1
            else:
                cursor_y += 1
        elif char == curses.KEY_LEFT and cursor_x > 0:
            cursor_x -= 1
        elif char == curses.KEY_RIGHT and cursor_x < screen_width - 1:
            cursor_x += 1
        elif char == ord('n'): # pagedown
            if session_monitor.check_highlight_location(1, move_step):
                cursor_y = CURSOR_Y_START + 1
        elif char == ord('N'): # pageup
            if session_monitor.check_highlight_location(-1, move_step):
                cursor_y = min(session_monitor.height + CURSOR_Y_START - 1, screen_height - 1)
        elif char == ord('p'): # print more session related details
            session_monitor.print_more_details(stdscr)
        elif char == ord('k') and Config.get("main.support_terminate"): # terminate single selected session
            session_monitor.terminate_selected_session(stdscr)
        elif char == ord('K') and Config.get("main.support_terminate"): # terminate all sessions with same sql id
            session_monitor.terminate_all_sessions(stdscr)
        elif char == ord('t'): # order by time
            session_monitor.refresh_by_elapsed_time()
        elif char == ord('m'): # order by memory
            session_monitor.refresh_by_pga()
        elif char == ord('e'): # order by event
            session_monitor.refresh_by_event()
        else:
            stdscr.move(0, 0)
            session_monitor.reset_print_location()
            curses.flushinp()
            return

        stdscr.move(cursor_y, cursor_x)
        session_monitor.print(stdscr)

def handle_emergency_related_keys(stdscr, emergency_module):
    curr_pos_y, curr_pos_x = constants.EMER_CURSOR_Y_START, constants.EMER_CURSOR_X_START
    screen_height, screen_width = stdscr.getmaxyx()
    while True:
        char = stdscr.getch()
        curses.flushinp()
        if char == ord('e'):
            pass
        elif char == curses.KEY_UP:
            if curr_pos_y > constants.EMER_CURSOR_Y_START:
                curr_pos_y -= 1
        elif char == curses.KEY_DOWN:
            if curr_pos_y < min(constants.EMER_CURSOR_Y_START + emergency_module.height, screen_height - 1):
                curr_pos_y += 1
        elif char == curses.KEY_LEFT:
            if curr_pos_x > 0:
                curr_pos_x -= 1
        elif char == curses.KEY_RIGHT:
            if curr_pos_x < screen_width - 1:
                curr_pos_x += 1
        elif char == ord('k') and Config.get("main.support_terminate"):
            emergency_module.emergency_handle_command_entry(stdscr, char)
        else:
            stdscr.move(0, 0)
            curses.flushinp()
            return

        stdscr.move(curr_pos_y, curr_pos_x)
        emergency_module.emergency_print(stdscr)

def handle_memory_related_keys(stdscr, memory_monitor):
    curr_pos_y, curr_pos_x = constants.MEM_CURSOR_Y_START, constants.MEM_CURSOR_X_START
    screen_height, screen_width = stdscr.getmaxyx()
    while True:
        char = stdscr.getch()
        curses.flushinp()
        if char == ord('m'):
            pass
        elif char == curses.KEY_UP:
            if curr_pos_y > constants.MEM_CURSOR_Y_START:
                curr_pos_y -= 1
        elif char == curses.KEY_DOWN:
            if curr_pos_y < min(constants.MEM_CURSOR_Y_START + memory_monitor.height, screen_height - 1):
                curr_pos_y += 1
        elif char == curses.KEY_LEFT:
            if curr_pos_x > 0:
                curr_pos_x -= 1
        elif char == curses.KEY_RIGHT:
            if curr_pos_x < screen_width - 1:
                curr_pos_x += 1
        elif char == ord('k') and Config.get("main.support_terminate"):
            memory_monitor.terminate_session_or_thread(stdscr)
        else:
            stdscr.move(0, 0)
            curses.flushinp()
            return

        stdscr.move(curr_pos_y, curr_pos_x)
        memory_monitor.print(stdscr)

def monitors_refresh(monitors_array):
    # multi threads refresh
    refresh_threads = []
    for monitor in monitors_array:
        thread = threading.Thread(target=util.refresh_analyze_wrapper,
                                  args=(monitor.logger, monitor.name, monitor.refresh))
        thread.start()
        refresh_threads.append(thread)

    for thread in refresh_threads:
        thread.join()

class DbtopRefresher(threading.Thread):
    def __init__(self):
        super().__init__()
        self.condition = threading.Condition()
        self.paused = False
        self.stop_flag = False
        self.monitors_array = None
        self.session_monitor = None
        self.memory_monitor = None
        self.emergency_module = None

    def init(self, monitors_array, session_monitor, memory_monitor, emergency_module):
        self.monitors_array = monitors_array
        self.session_monitor = session_monitor
        self.memory_monitor = memory_monitor
        self.emergency_module = emergency_module

    def run(self):
        while not self.stop_flag:
            with self.condition:
                while self.paused:
                    self.condition.wait()

            if self.stop_flag:
                return

            # record start time
            start_time = time.perf_counter()

            # start refresh
            monitors_refresh(self.monitors_array)

            # emergency main routine
            if self.emergency_module:
                monitors_value = {}
                for monitor in self.monitors_array:
                    monitors_value[monitor.name] = super(type(monitor), monitor).get_monitor_value()
                if self.memory_monitor is not None:
                    monitors_value[self.memory_monitor.name] = self.memory_monitor.get_monitor_panels()

                self.emergency_module.emergency_main(monitors_value, self.session_monitor.get_session())
                self.session_monitor.set_trigger_emergency_sql_ids(self.emergency_module.get_trigger_emergency_sql_ids())
                self.session_monitor.set_trigger_emergency_pids(self.emergency_module.get_trigger_emergency_pids())
                if self.memory_monitor is not None:
                    self.memory_monitor.set_memory_full_type(self.emergency_module.get_memory_full_type())

            # update refresh time
            util.update_refresh_time()

            # calculate sleep time
            elapsed_time = time.perf_counter() - start_time
            sleep_time = max(0, Config.get("main.interval") - elapsed_time)
            if sleep_time > 0:
                time.sleep(sleep_time)

    def pause(self):
        with self.condition:
            self.paused = True

    def resume(self):
        with self.condition:
            self.paused = False
            self.condition.notify()

    def stop(self):
        self.stop_flag = True
        self.resume()

def switch_to_memory_view(monitors_array, memory_monitor):
    for monitor in monitors_array:
        monitor.print_to_screen = False

    memory_monitor.print_to_screen = True
    return True

def switch_to_normal_view(monitors_array, memory_monitor):
    for monitor in monitors_array:
        monitor.print_to_screen = True

    memory_monitor.print_to_screen = False
    return False

def dbtop_main_routine(stdscr):
    # start alarm
    alarm.start_alarm()

    if stdscr is not None:
        # set curses
        curses.curs_set(0)
        curses.cbreak()
        curses.start_color()
        curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLACK)  # white font black background
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_WHITE)  # black font white background
        curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)  # red font black background
        curses.init_pair(4, curses.COLOR_RED, curses.COLOR_WHITE)  # red font white background
        curses.init_pair(5, curses.COLOR_YELLOW, curses.COLOR_BLACK)  # yellow font black background
        curses.init_pair(6, curses.COLOR_GREEN, curses.COLOR_BLACK)  # green font black background
        curses.init_pair(7, curses.COLOR_CYAN, curses.COLOR_BLACK)  # cyan font black background

    # init each monitor
    db_monitor = db.DBMonitor()
    os_monitor = operating_system.OSMonitor()
    instance_monitor = instance.InsMonitor()
    event_monitor = event.EventMonitor()
    session_monitor = session.SessionMonitor()
    monitors_array = [db_monitor, os_monitor, instance_monitor, event_monitor, session_monitor]

    begin_y = 0
    for monitor in monitors_array:
        monitor.init(0, begin_y, MONITOR_WIDTH + 1)
        begin_y += monitor.height

    # init memory monitor
    memory_monitor = None
    if Config.get("main.mem_interval") != 0:
        memory_monitor = memory.MemoryMonitor()
        memory_monitor.stdscr = stdscr
        memory_monitor.init(0, 0, MONITOR_WIDTH + 1)

    # init emergency module
    emergency_module = None
    if Config.get("emergency.enable"):
        emergency_module = emergency.EmergencyMain(0, begin_y, MONITOR_WIDTH + 1)

    if stdscr is not None:
        # set keypad
        stdscr.keypad(True)

        stdscr.nodelay(True)
        stdscr.getch()
        stdscr.nodelay(False)
        stdscr.timeout(Config.get("main.interval") * 1000)

    if Config.get("main.log_interval") != 0:
        # message queue between monitor and log thread
        data_queue = queue.LifoQueue()
        db_monitor.set_message_queue(data_queue)
        os_monitor.set_message_queue(data_queue)
        instance_monitor.set_message_queue(data_queue)

        db_info = data_logger.DBInfo()
        db_monitor.set_db_info_container(db_info)
        db_monitor.refresh()

        # init logger for db/os/instance monitor
        logger = data_logger.DataLogger(db_info, data_queue, Config.get("main.log_interval"))
        logger.start()
    else:
        monitors_refresh(monitors_array)

    # start refresh thread
    refresh_thread = DbtopRefresher()
    refresh_thread.init(monitors_array, session_monitor, memory_monitor, emergency_module)
    refresh_thread.start()

    show_memory_view = False
    try:
        while True:
            # check DbtopRefresher status
            if util.should_exit():
                app_logger.error("DBTOP needs to exit because DbtopRefresher has not refresh data for 5 minutes.")
                # report alarm
                key = "DbtopRefresher"
                value = "DBTOP检测到工具的刷新线程超过5分钟未刷新数据，现在自动退出进程等待重新拉起"
                alarm.check_and_report_alarm(app_logger, key, value, True)
                return

            # print
            monitors_dump_data = []
            for monitor in monitors_array:
                with monitor.lock:
                    monitor.print(stdscr)
                    if Config.get("emergency.enable"):
                        monitors_dump_data.append(super(type(monitor), monitor).get_dump_data())
                    else:
                        super(type(monitor), monitor).clear_dump_data()

            # emergency print and persist
            if Config.get("emergency.enable"):
                emergency_module.emergency_print(stdscr)
                emergency_module.emergency_persist(monitors_dump_data)

            if stdscr is None:
                time.sleep(Config.get("main.interval"))
                continue

            # handle command from user
            char = stdscr.getch()

            if show_memory_view:
                if char == ord('q'):
                    show_memory_view = switch_to_normal_view(monitors_array, memory_monitor)
                elif char == ord('m'):
                    handle_memory_related_keys(stdscr, memory_monitor)
                else:
                    curses.flushinp()
                continue

            if char == ord('q'):
                break
            elif char == ord('r'):
                event_monitor.immediate = True
            elif char == ord('c'):
                event_monitor.immediate = False
            elif char == ord('m'):
                if Config.get("main.mem_interval") != 0:
                    show_memory_view = switch_to_memory_view(monitors_array, memory_monitor)
                    memory_monitor.print(stdscr)
            elif char == ord('e'):
                if Config.get("emergency.enable") and emergency_module.emergency_triggered:
                    refresh_thread.pause()
                    # set unlimited wait time
                    stdscr.timeout(-1)
                    handle_emergency_related_keys(stdscr, emergency_module)
                    # recover to normal wait time
                    stdscr.timeout(Config.get("main.interval") * 1000)
                    refresh_thread.resume()
            elif char == ord('s'):
                refresh_thread.pause()
                # set unlimited wait time
                stdscr.timeout(-1)
                handle_session_related_keys(stdscr, session_monitor)
                # recover to normal wait time
                stdscr.timeout(Config.get("main.interval") * 1000)
                refresh_thread.resume()
            else:
                curses.flushinp()
                continue
    except KeyboardInterrupt:
        app_logger.warning("DBTOP receive KeyboardInterrupt.")
    except Exception as e:
        app_logger.error("DBTOP Exception Traceback:\n%s", traceback.format_exc())
    finally:
        app_logger.warning("DBTOP is starting to exit.")
        # stop data log thread
        if Config.get("main.log_interval") != 0:
            logger.stop()
        # stop memory refresh thread
        if Config.get("main.mem_interval") != 0:
            memory_monitor.stop()
        # stop alarm
        alarm.stop_alarm()
        # stop refresh thread
        refresh_thread.stop()
        # exit
        sys.exit(0)

def _resolve_config_path():
    """Resolve dbtop.cfg path: check CWD first, then package directory."""
    # 1. current working directory
    cwd_config = os.path.join(os.getcwd(), "dbtop.cfg")
    if os.path.exists(cwd_config):
        return cwd_config

    # 2. same directory as this script (pip install puts dbtop.cfg here via package_data)
    script_dir_config = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbtop.cfg")
    if os.path.exists(script_dir_config):
        return script_dir_config

    # 3. project root (parent of tool/)
    proj_config = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbtop.cfg")
    if os.path.exists(proj_config):
        return proj_config

    # fallback
    return "dbtop.cfg"


def main():
    global app_logger

    # parse options
    parser = argparse.ArgumentParser(
        description="DBTOP V1.0\nOracle Database Real-time Monitor",
        epilog=constants.DETAIL_HELP,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-i', '--interval', dest='interval', type=int, help='refresh interval')
    parser.add_argument('-l', '--log_interval', dest='log_interval', type=int, help='log refresh interval')
    parser.add_argument('-u', '--user', dest='user', type=str, help=f'database user')
    parser.add_argument('-H', '--host', dest='host', type=str, help=f'database host address')
    parser.add_argument('-p', '--port', dest='port', type=int, help=f'database listener port')
    parser.add_argument('-s', '--service_name', dest='service_name', type=str, help=f'Oracle service name')
    parser.add_argument('-d', '--daemon', action='store_true', dest='daemon', default=False, help='run dbtop in daemon mode')
    args = parser.parse_args()

    config_path = _resolve_config_path()
    Config.init_instance(config_path, args)
    # disable terminate session
    Config.set("main.support_terminate", False)

    # control the number of concurrent users (only match actual python processes, not su/bash wrappers)
    get_dbtop_pid_cmd = "ps -eo pid,comm,args | awk '$2 ~ /^python/ && (/tool\\.dbtop/ || /dbtop:main/) {print $1}'"
    if hasattr(sys, '_MEIPASS'):  # packed by PyInstaller
        get_dbtop_pid_cmd = "ps -eo pid,comm,args | awk '$2 ~ /^dbtop/ {print $1}'"

    dbtop_pids = os.popen(get_dbtop_pid_cmd).read().strip()
    if dbtop_pids:
        pid_set = set(dbtop_pids.split())
        if Config.get("main.daemon"):
            if len(pid_set) > 1:
                print("The backend user limit has been reached, exit.")
                sys.exit(0)
        else:
            if len(pid_set) > Config.get("main.max_concurrent_users"):
                print("The user limit has been reached, exit.")
                sys.exit(0)

    # sysdba mode does not need password; non-sysdba reads password from config or prompts
    if not Config.get("main.sysdba"):
        db_password = Config.get("main.password")
        if db_password:
            Config.set("main.db_password", db_password)
        else:
            db_password = getpass.getpass("Please input database user password: ")
            print("Input password successfully.")
            Config.set("main.db_password", db_password)

    app_logger = log.Logger(name='dbtop', log_file='dbtop_app.log')

    # start main routine
    if not Config.get("main.daemon"):
        curses.wrapper(dbtop_main_routine)
    else:
        dbtop_main_routine(None)


if __name__ == "__main__":
    main()
