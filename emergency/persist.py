# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

try:
    import oracledb
except ImportError:
    import cx_Oracle as oracledb
import time
from functools import wraps
from common import log
from common.config import Config

class Persist:
    def __init__(self):
        # create logger
        self.logger = log.Logger(name='persist', log_file='dbtop_emergency_run.log', level='INFO')
        # create db connection
        dsn = oracledb.makedsn(
            host=Config.get("main.host"),
            port=Config.get("main.port"),
            service_name=Config.get("main.service_name")
        )
        self.connection = oracledb.connect(
            user=Config.get("main.user"),
            password=Config.get("main.db_password"),
            dsn=dsn
        )
        self.connection.autocommit = True
        # create table and sequence
        self.create_table()
        self.create_snap_id_sequence()
        self.persist_sql_info = self.log_slow_function()(self.persist_sql_info)
        self.persist_emergency_sql_info = self.log_slow_function()(self.persist_emergency_sql_info)
        self.persist_ins_info = self.log_slow_function()(self.persist_ins_info)
        self.get_snap_id = self.log_slow_function()(self.get_snap_id)
        self.get_sql_info_snap = self.log_slow_function()(self.get_sql_info_snap)
        self.get_ins_info_snap = self.log_slow_function()(self.get_ins_info_snap)
        self.get_emergency_sql_info_snap = self.log_slow_function()(self.get_emergency_sql_info_snap)
        self.get_emergency_sql_unrecovered = self.log_slow_function()(self.get_emergency_sql_unrecovered)
        self.update_emergency_sql_recovered = self.log_slow_function()(self.update_emergency_sql_recovered)

    def log_slow_function(self):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                result = func(*args, **kwargs)
                elapsed_time = time.time() - start_time
                if elapsed_time > 0.2: # 200ms
                    self.logger.warning(f"Slow func: {func.__name__}  Time used: {elapsed_time:.3f}s  Args: args={args}, kwargs={kwargs}")
                return result
            return wrapper
        return decorator

    def create_table(self):
        # create snap_ins_info table
        query = """
            DECLARE
                v_cnt NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_cnt FROM user_tables WHERE table_name = 'SNAP_INS_INFO';
                IF v_cnt = 0 THEN
                    EXECUTE IMMEDIATE '
                        CREATE TABLE snap_ins_info (
                            db_id           NUMBER,
                            snap_id         NUMBER PRIMARY KEY,
                            snap_ts         TIMESTAMP,
                            ins_acs_cnt     NUMBER,
                            ins_cpu_utl     NUMBER
                        ) PARTITION BY RANGE (snap_ts)
                        INTERVAL(NUMTOYMINTERVAL(1,''MONTH''))
                        (PARTITION P1 VALUES LESS THAN (TIMESTAMP ''2025-06-01 00:00:00''))';
                END IF;
            END;
            """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
        except Exception as e:
            self.logger.warning("Create table 'snap_ins_info' failed: %s", e)

        # create snap_sql_info table
        query = """
            DECLARE
                v_cnt NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_cnt FROM user_tables WHERE table_name = 'SNAP_SQL_INFO';
                IF v_cnt = 0 THEN
                    EXECUTE IMMEDIATE '
                        CREATE TABLE snap_sql_info (
                            db_id           NUMBER,
                            snap_id         NUMBER,
                            snap_ts         TIMESTAMP,
                            unique_sql_id   VARCHAR2(30),
                            sql_acs_cnt     NUMBER,
                            sql_latency     NUMBER,
                            sql_cputime     NUMBER,
                            sql_qps         NUMBER
                        ) PARTITION BY RANGE (snap_ts)
                        INTERVAL(NUMTOYMINTERVAL(1,''MONTH''))
                        (PARTITION P1 VALUES LESS THAN (TIMESTAMP ''2025-06-01 00:00:00''))';
                    EXECUTE IMMEDIATE 'CREATE INDEX snap_sql_info_idx ON snap_sql_info (snap_id)';
                END IF;
            END;
            """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
        except Exception as e:
            self.logger.warning("Create table 'snap_sql_info' failed: %s", e)

        # create snap_emergency_sql_info table
        query = """
            DECLARE
                v_cnt NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_cnt FROM user_tables WHERE table_name = 'SNAP_EMERGENCY_SQL_INFO';
                IF v_cnt = 0 THEN
                    EXECUTE IMMEDIATE '
                        CREATE TABLE snap_emergency_sql_info (
                            db_id           NUMBER,
                            snap_id         NUMBER,
                            snap_ts         TIMESTAMP,
                            unique_sql_id   VARCHAR2(30),
                            sql_acs_cnt     NUMBER,
                            sql_latency     NUMBER,
                            sql_cputime     NUMBER,
                            sql_qps         NUMBER,
                            emergency_ts    TIMESTAMP,
                            recovered       NUMBER(1) DEFAULT 0
                        ) PARTITION BY RANGE (snap_ts)
                        INTERVAL(NUMTOYMINTERVAL(1,''MONTH''))
                        (PARTITION P1 VALUES LESS THAN (TIMESTAMP ''2025-06-01 00:00:00''))';
                    EXECUTE IMMEDIATE 'CREATE INDEX snap_emergency_sql_info_idx ON snap_emergency_sql_info (snap_id)';
                END IF;
            END;
            """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()
        except Exception as e:
            self.logger.warning("Create table 'snap_emergency_sql_info' failed: %s", e)

    def persist_sql_info(self, sql_info):
        try:
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO snap_sql_info VALUES(:1, :2, TO_TIMESTAMP(:3, 'YYYY-MM-DD HH24:MI:SS'), :4, :5, :6, :7, :8)",
                            (sql_info["db_id"], sql_info["snap_id"], sql_info["snap_ts"], sql_info["unique_sql_id"],
                            sql_info["sql_acs_cnt"], sql_info["sql_latency"], sql_info["sql_cputime"], sql_info["sql_qps"]))
            cursor.close()
            return
        except Exception as e:
            self.logger.error(f"Persist sql info failed: {e}")
            return

    def persist_emergency_sql_info(self, sql_info):
        try:
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO snap_emergency_sql_info VALUES(:1, :2, TO_TIMESTAMP(:3, 'YYYY-MM-DD HH24:MI:SS'), :4, :5, :6, :7, :8, TO_TIMESTAMP(:9, 'YYYY-MM-DD HH24:MI:SS'), :10)",
                            (sql_info["db_id"], sql_info["snap_id"], sql_info["snap_ts"], sql_info["unique_sql_id"],
                            sql_info["sql_acs_cnt"], sql_info["sql_latency"], sql_info["sql_cputime"], sql_info["sql_qps"],
                            sql_info["emergency_ts"], 0 if sql_info["recovered"] == False else 1))
            cursor.close()
            return
        except Exception as e:
            self.logger.error(f"Persist emergency sql info failed: {e}")
            return

    def persist_ins_info(self, ins_info):
        try:
            cursor = self.connection.cursor()
            cursor.execute("INSERT INTO snap_ins_info VALUES(:1, :2, TO_TIMESTAMP(:3, 'YYYY-MM-DD HH24:MI:SS'), :4, :5)",
                            (ins_info["db_id"], ins_info["snap_id"], ins_info["snap_ts"],
                            ins_info["ins_acs_cnt"], ins_info["ins_cpu_utl"]))
            cursor.close()
            return
        except Exception as e:
            self.logger.error(f"Persist ins info failed: {e}")
            return

    def create_snap_id_sequence(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                DECLARE
                    v_cnt NUMBER;
                BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM user_sequences WHERE sequence_name = 'GLOBAL_SNAP_ID';
                    IF v_cnt = 0 THEN
                        EXECUTE IMMEDIATE 'CREATE SEQUENCE global_snap_id';
                    END IF;
                END;
            """)
            cursor.close()
            return
        except Exception as e:
            self.logger.warning(f"Create sequence failed: {e}")
            return

    def get_snap_id(self):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT global_snap_id.NEXTVAL FROM DUAL")
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self.logger.error(f"Query sequence failed: {e}")
            return None

    def get_sql_info_snap(self, db_id, snap_id, unique_sql_id):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM snap_sql_info WHERE db_id=:1 AND snap_id BETWEEN :2 AND :3 AND unique_sql_id=:4 ORDER BY snap_id DESC",
                           (db_id, snap_id - 10, snap_id - 1, unique_sql_id))
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self.logger.error(f"Query sql info snapshot failed: {e}")
            return None

    def get_ins_info_snap(self, db_id, snap_id):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM snap_ins_info WHERE db_id=:1 AND snap_id=:2", (db_id, snap_id))
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self.logger.error(f"Query ins info snapshot failed: {e}")
            return None

    def get_emergency_sql_info_snap(self, db_id, unique_sql_id):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM snap_emergency_sql_info WHERE db_id=:1 AND unique_sql_id=:2 AND recovered=0", (db_id, unique_sql_id))
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self.logger.error(f"Query emergency sql info snapshot failed: {e}")
            return None

    def get_emergency_sql_unrecovered(self, db_id):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM snap_emergency_sql_info WHERE db_id=:1 AND recovered=0", (db_id, ))
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            self.logger.error(f"Query unrecovered emergency sql failed: {e}")
            return None

    def update_emergency_sql_recovered(self, db_id, snap_id, unique_sql_id):
        try:
            cursor = self.connection.cursor()
            cursor.execute("UPDATE snap_emergency_sql_info SET recovered=1 WHERE db_id=:1 AND snap_id=:2 AND unique_sql_id=:3", (db_id, snap_id, unique_sql_id))
            cursor.close()
            return
        except Exception as e:
            self.logger.error(f"Update emergency sql recovered failed: {e}")
            return
