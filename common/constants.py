# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
常量定义模块 (Constants)

集中管理 dbtop 的所有常量，包括:
    - 界面布局参数：应急模块和内存模块的起始坐标
    - 帮助文档：DETAIL_HELP 包含完整的快捷键说明和各监控面板字段含义
    - 会话终止 PL/SQL 匿名块：用于批量终止 Oracle 会话的 4 种模板
      * TERMINATE_LIMITED_SESSIONS — 按 SQL_ID 终止指定数量的会话
      * TERMINATE_LIMITED_SESSIONS_WITHTIME — 仅终止执行超时的会话（限制数量）
      * TERMINATE_UNLIMITED_SESSIONS — 按 SQL_ID 终止所有匹配会话
      * TERMINATE_UNLIMITED_SESSIONS_WITHTIME — 终止所有超时的匹配会话
"""

EMER_CURSOR_Y_START = 44
EMER_CURSOR_X_START = 0

MEM_CURSOR_Y_START = 10
MEM_CURSOR_X_START = 0

DETAIL_HELP = ("Combined argument example:\n"
               "-u [USER] -H [HOST] -p [PORT] -s [SERVICE_NAME] -i [INTERVAL]\n"
               "e.g. dbtop -u system -H 192.168.1.100 -p 1521 -s orcl -i 5\n"
               "\n"
               "\n"
               "Hot Keys:\n"
               "   Cmd\tDescription\n"
               "   r\t以实时方式展示事件监控数据\n"
               "   c\t以累计方式展示事件监控数据\n"
               "   s\t进入会话选择状态，需配合下面短命令使用\n"
               "        方向键: 通过键盘方向键逐行移动选择会话\n"
               "        n: 向下翻页\n"
               "        N: 向上翻页\n"
               "        t: 以会话执行的SQL语句耗时进行排序展示会话数据\n"
               "        m: 以会话的内存占用进行排序展示会话数据\n"
               "        e: 以会话的等待事件进行排序展示会话数据\n"
               "        k: 一键终结当前选择的会话\n"
               "        K: 一键终结和当前会话SQL ID相同的所有会话\n"
               "        p: 展示会话详细信息（SQL ID、SQL文本、客户端信息、执行计划、阻塞树等）\n"
               "   e\t进入应急预案选择状态\n"
               "        方向键: 通过键盘方向键逐行移动选择内容\n"
               "   q\t退出工具\n"
               "\n"
               "\n"
               "DESCRIPTION FOR EACH MONITOR\n"
               "PART I -- DB Monitor\n"
               "   * Oracle数据库版本\n"
               "   * 当前工具登录数据库的用户名\n"
               "   * 当前时间: 格式为yyyy-mm-dd hh:mm:ss\n"
               "   * 数据库启动时间\n"
               "   * ROLE: 数据库角色（PRIMARY / PHYSICAL STANDBY）\n"
               "   * SGA: SGA内存总用量（MB）\n"
               "   * PGA: PGA内存总分配量（MB）\n"
               "   * db%: 数据库繁忙度\n"
               "   * WTR%: 等待时间占比\n"
               "PART II -- OS Monitor\n"
               "   * LOAD: 当前节点上操作系统最近一分钟负载\n"
               "   * %%CPU: 当前节点上操作系统CPU使用率\n"
               "   * %%MEM: 当前节点内存使用率\n"
               "   * r/s: 每秒IO读取次数\n"
               "   * w/s: 每秒IO写入次数\n"
               "   * rkB/s: 每秒IO读取大小（单位：kB）\n"
               "   * wkB/s: 每秒IO写入大小（单位：kB）\n"
               "   * r_await: 平均每次读请求等待时间（单位：毫秒）\n"
               "   * w_await: 平均每次写请求等待时间（单位：毫秒）\n"
               "   * r_asize(kB): 平均每次读IO大小（单位：kB）\n"
               "   * w_asize(kB): 平均每次写IO大小（单位：kB）\n"
               "   * aqu-sz: 磁盘请求队列的平均长度\n"
               "PART III -- INSTANCE Monitor\n"
               "   * SN: 当前连接数据库的会话数\n"
               "   * AN: 当前非空闲等待的会话数\n"
               "   * ASC: 正在执行SQL的活跃会话数（ON CPU）\n"
               "   * ASI: 正在等待IO的活跃会话数\n"
               "   * IDL: 处于空闲等待的会话数\n"
               "   * MBPS: 数据库进程的IO读写量\n"
               "   * TPS: 数据库当前TPS\n"
               "   * QPS: 数据库当前QPS\n"
               "   * P95(ms): 数据库最近SQL的P95时延\n"
               "   * REDO(kB/s): Redo日志生成速率\n"
               "   * CONNECTION(c/m): 连接数使用率\n"
               "   * PROCESSES: 进程使用率\n"
               "PART IV -- EVENT Monitor\n"
               "   * EVENT(RT/C): 等待事件的名称\n"
               "   * TOTAL WAITS: 等待事件在统计周期内出现的次数\n"
               "   * TIME(us): 等待事件在统计周期内的耗时（单位：微秒）\n"
               "   * AVG(us): 等待事件的平均单次耗时（单位：微秒）\n"
               "   * PCT: 等待事件的耗时占总耗时的比例\n"
               "   * WAIT_CLASS: 等待事件所属的类别\n"
               "PART V -- SESSION Monitor\n"
               "   * SID: Oracle会话ID\n"
               "   * USR: 登录该会话的用户名\n"
               "   * PROG: 连接到该会话的应用程序名\n"
               "   * PGA(m): 会话的PGA内存使用量\n"
               "   * SQLID: SQL语句对应的SQL_ID\n"
               "   * SQL: SQL语句文本\n"
               "   * OPN: SQL语句对应的操作类型\n"
               "   * BLOCKER: 阻塞当前会话的SID\n"
               "   * E/T(ms): 当前SQL语句的耗时（单位：毫秒）\n"
               "   * STA: 当前会话状态\n"
               "   * STE: 会话状态（'ON CPU' 'USR I/O' 'WAITING'）\n"
               "   * EVENT: 会话上报的等待事件\n"
               "   * SParse: SQL语句的软解析率\n"
               "   * BLK: 会话的阻塞状态（\"H\"表示锁占有者 \"W\"表示锁等待者 \"H&W\"表示两者都是）"
            )

# Oracle PL/SQL anonymous blocks for batch session termination
TERMINATE_LIMITED_SESSIONS_ANONYMOUS_BLOCK = """
DECLARE
    v_sql_id      VARCHAR2(30) := '{sql_id}';
    v_max_count   NUMBER := {max_terminate_count};
    v_count       NUMBER := 0;
BEGIN
    FOR rec IN (
        SELECT s.SID, s.SERIAL#
        FROM v$session s
        WHERE s.SQL_ID = v_sql_id
        ORDER BY s.SQL_EXEC_START NULLS LAST
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';
            DBMS_OUTPUT.PUT_LINE('Killed SID: ' || rec.SID || ' SERIAL#: ' || rec.SERIAL#);
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Kill SID: ' || rec.SID || ' failed: ' || SQLERRM);
        END;
        IF v_count >= v_max_count THEN
            EXIT;
        END IF;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('Total killed: ' || v_count);
END;
"""

TERMINATE_LIMITED_SESSIONS_WITHTIME_ANONYMOUS_BLOCK = """
DECLARE
    v_sql_id      VARCHAR2(30) := '{sql_id}';
    v_max_count   NUMBER := {max_terminate_count};
    v_min_secs    NUMBER := {timeout_thresh_secs};
    v_count       NUMBER := 0;
BEGIN
    FOR rec IN (
        SELECT s.SID, s.SERIAL#
        FROM v$session s
        WHERE s.SQL_ID = v_sql_id
          AND s.STATUS = 'ACTIVE'
          AND s.SQL_EXEC_START IS NOT NULL
          AND (SYSDATE - s.SQL_EXEC_START) * 86400 > v_min_secs
        ORDER BY s.SQL_EXEC_START ASC
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
        IF v_count >= v_max_count THEN
            EXIT;
        END IF;
    END LOOP;
END;
"""

TERMINATE_UNLIMITED_SESSIONS_ANONYMOUS_BLOCK = """
DECLARE
    v_sql_id VARCHAR2(30) := '{sql_id}';
    v_count  NUMBER := 0;
BEGIN
    FOR rec IN (
        SELECT s.SID, s.SERIAL#
        FROM v$session s
        WHERE s.SQL_ID = v_sql_id
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;
"""

TERMINATE_UNLIMITED_SESSIONS_WITHTIME_ANONYMOUS_BLOCK = """
DECLARE
    v_sql_id   VARCHAR2(30) := '{sql_id}';
    v_min_secs NUMBER := {timeout_thresh_secs};
    v_count    NUMBER := 0;
BEGIN
    FOR rec IN (
        SELECT s.SID, s.SERIAL#
        FROM v$session s
        WHERE s.SQL_ID = v_sql_id
          AND s.STATUS = 'ACTIVE'
          AND s.SQL_EXEC_START IS NOT NULL
          AND (SYSDATE - s.SQL_EXEC_START) * 86400 > v_min_secs
        ORDER BY s.SQL_EXEC_START ASC
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION ''' || rec.SID || ',' || rec.SERIAL# || ''' IMMEDIATE';
            v_count := v_count + 1;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;
END;
"""
