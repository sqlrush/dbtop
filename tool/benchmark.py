#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush
"""
Oracle Database TPS/QPS Benchmark Tool
用法: python3 benchmark.py [--processes N] [--duration SEC] [--mode tps|qps]
"""

import argparse
import ctypes
import multiprocessing
import random
import signal
import time

try:
    import oracledb
except ImportError:
    import cx_Oracle as oracledb

# QPS 模式的 PL/SQL 批量查询
QPS_BATCH_SIZE = 2000
PLSQL_QPS = """
DECLARE
    v_id NUMBER;
    v_val VARCHAR2(100);
    v_num NUMBER;
BEGIN
    FOR i IN 1..%d LOOP
        v_id := TRUNC(DBMS_RANDOM.VALUE(1, 1000001));
        SELECT val, num INTO v_val, v_num FROM bench_test WHERE id = v_id;
    END LOOP;
END;
""" % QPS_BATCH_SIZE

# TPS 模式的 SQL 模板（Python 层面逐条执行 + commit，确保 user commits 计数）
UPDATE_SQL = "UPDATE bench_test SET num = :1 WHERE id = :2"
INSERT_SQL = "INSERT INTO bench_test(id, val, num) VALUES (bench_seq.NEXTVAL, 'B', :1)"


TPS_BATCH_SIZE = 300
PLSQL_TPS = """
DECLARE
    v_id NUMBER;
    v_op NUMBER;
BEGIN
    FOR i IN 1..%d LOOP
        v_op := MOD(i, 10);
        v_id := TRUNC(DBMS_RANDOM.VALUE(1, 1000001));
        IF v_op = 0 THEN
            BEGIN
                INSERT INTO bench_test(id, val, num) VALUES (bench_seq.NEXTVAL, 'B', i);
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        ELSE
            UPDATE bench_test SET num = i WHERE id = v_id;
        END IF;
        COMMIT WRITE BATCH NOWAIT;
    END LOOP;
END;
""" % TPS_BATCH_SIZE


def tps_worker(worker_id, shared_counter, shared_stop, dsn, user, password, sysdba):
    """TPS worker: PL/SQL 批量 DML + COMMIT (系统级 BATCH,NOWAIT 生效)"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        if sysdba:
            conn = oracledb.connect(mode=oracledb.SYSDBA)
        else:
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
        conn.autocommit = False
        cursor = conn.cursor()
    except Exception as e:
        print("[Worker-%d] Connection failed: %s" % (worker_id, e))
        return

    try:
        while not shared_stop.value:
            cursor.execute(PLSQL_TPS)
            with shared_counter.get_lock():
                shared_counter.value += TPS_BATCH_SIZE
    except Exception as e:
        if not shared_stop.value:
            print("[Worker-%d] Error: %s" % (worker_id, e))
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def qps_worker(worker_id, shared_counter, shared_stop, dsn, user, password, sysdba):
    """QPS worker: PL/SQL 批量查询"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        if sysdba:
            conn = oracledb.connect(mode=oracledb.SYSDBA)
        else:
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print("[Worker-%d] Connection failed: %s" % (worker_id, e))
        return

    try:
        while not shared_stop.value:
            cursor.execute(PLSQL_QPS)
            with shared_counter.get_lock():
                shared_counter.value += QPS_BATCH_SIZE
    except Exception as e:
        if not shared_stop.value:
            print("[Worker-%d] Error: %s" % (worker_id, e))
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Oracle TPS/QPS Benchmark")
    parser.add_argument('-n', '--processes', type=int, default=multiprocessing.cpu_count() * 4,
                        help='number of worker processes (default: cpu*4)')
    parser.add_argument('-t', '--duration', type=int, default=300, help='duration in seconds (default: 300)')
    parser.add_argument('-m', '--mode', type=str, default='tps', choices=['tps', 'qps'],
                        help='benchmark mode: tps (INSERT/UPDATE with real commits) or qps (SELECT)')
    parser.add_argument('-u', '--user', type=str, default='system', help='database user')
    parser.add_argument('-p', '--password', type=str, default='', help='database password')
    parser.add_argument('-H', '--host', type=str, default='localhost', help='database host')
    parser.add_argument('-P', '--port', type=int, default=1521, help='database port')
    parser.add_argument('-s', '--service', type=str, default='orcl', help='Oracle service name')
    parser.add_argument('--sysdba', action='store_true', default=True, help='connect as SYSDBA (default)')
    parser.add_argument('--no-sysdba', action='store_false', dest='sysdba', help='connect with user/password')
    args = parser.parse_args()

    label = "TPS" if args.mode == 'tps' else "QPS"
    dsn = oracledb.makedsn(args.host, args.port, service_name=args.service)
    worker_fn = tps_worker if args.mode == 'tps' else qps_worker

    if args.mode == 'tps':
        print("Oracle TPS Benchmark (INSERT 10%% / UPDATE 90%%)")
        print("")
        print("Optimizations applied:")
        print("  [1] commit_write    = BATCH,NOWAIT  (async redo flush)")
        print("  [2] commit_logging  = BATCH         (batch redo logging)")
        print("  [3] resource_manager_plan = ''       (no CPU throttling)")
        print("  [4] sequence cache  = 50000 NOORDER  (reduce sequence latch)")
        print("  [5] PL/SQL COMMIT + sys BATCH,NOWAIT   (fast commit, user commits counted)")
        print("  [6] 1M rows + HASH 32 partitions       (reduce buffer busy waits)")
        print("  [7] UPDATE on known rows (1-1000000)   (no empty transactions)")
        print("")
    else:
        print("Oracle QPS Benchmark (SELECT)")

    print("  Processes:    %d" % args.processes)
    print("  Duration:     %ds" % args.duration)
    print("  Connect mode: %s" % ('SYSDBA' if args.sysdba else '%s@%s' % (args.user, dsn)))

    shared_counter = multiprocessing.Value(ctypes.c_long, 0)
    shared_stop = multiprocessing.Value(ctypes.c_int, 0)

    workers = []
    for i in range(args.processes):
        p = multiprocessing.Process(
            target=worker_fn,
            args=(i, shared_counter, shared_stop, dsn, args.user, args.password, args.sysdba),
            daemon=True
        )
        p.start()
        workers.append(p)

    print("  Workers started: %d" % len(workers))
    print("")
    print("%6s  %14s  %14s  %16s" % ("Time", "Instant " + label, "Avg " + label, "Total Txns"))
    print("-" * 58)

    start_time = time.time()
    last_count = 0
    last_time = start_time

    try:
        while True:
            time.sleep(1)
            now = time.time()
            elapsed = now - start_time
            current_count = shared_counter.value

            dt = now - last_time
            instant = (current_count - last_count) / dt if dt > 0 else 0
            avg = current_count / elapsed if elapsed > 0 else 0

            print("%5.0fs  %14s  %14s  %16s" % (
                elapsed,
                "{:,.0f}".format(instant),
                "{:,.0f}".format(avg),
                "{:,d}".format(current_count)
            ))

            last_count = current_count
            last_time = now

            if elapsed >= args.duration:
                break
    except KeyboardInterrupt:
        print("\nStopping...")

    shared_stop.value = 1

    total_time = time.time() - start_time
    total = shared_counter.value
    print("-" * 58)
    print("Done. Total: {:,} txns in {:.1f}s, Avg {}: {:,.0f}".format(
        total, total_time, label, total / total_time if total_time > 0 else 0))

    for p in workers:
        p.join(timeout=3)
        if p.is_alive():
            p.terminate()


if __name__ == "__main__":
    main()
