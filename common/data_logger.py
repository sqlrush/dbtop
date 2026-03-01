# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

import logging
import logging.handlers
import os
import queue
import threading
from datetime import datetime
from time import sleep
import gzip
import shutil
import re
from .config import Config
from .log import Logger

class DBInfo:
    def __init__(self):
        self.current_version = "unknown"
        self.username = "system"
        self.role = "unknown"

class CompressedDynamicFileHandler(logging.handlers.RotatingFileHandler):
    def __init__(self, db_info, base_dir, max_bytes, backup_count):
        self.db_info = db_info
        self.base_dir = base_dir
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.run_logger = Logger(name='data', log_file='dbtop_app.log')

        os.makedirs(self.base_dir, exist_ok=True)

        self.current_filename = self._generate_filename()
        super().__init__(self.current_filename, maxBytes=max_bytes, backupCount=backup_count)

    def _generate_filename(self):
        counter = 0
        while True:
            version = re.sub(r'\s+', '_', self.db_info.current_version)
            username = self.db_info.username
            role = self.db_info.role
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if version != "unknown":
                break
            counter += 1
            assert counter <= 10
            sleep(1)

        return os.path.join(self.base_dir,
                            f"dbtoplog_Oracle_{version}_{username}_{role}_{timestamp}.log")

    def shouldRollover(self, record):
        if self.stream is None:
            self.stream = self._open()

        if self.max_bytes > 0:
            self.stream.seek(0, 2)
            if self.stream.tell() + len(record.getMessage()) > self.max_bytes:
                return 1

        return 0

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        if os.path.exists(self.current_filename):
            self._compress_file(self.current_filename)
            self._manage_backups()

        self.current_filename = self._generate_filename()

        self.baseFilename = self.current_filename
        self.stream = self._open()

    def _compress_file(self, filename):
        compressed_name = f"{filename}.gz"

        try:
            with open(filename, 'rb') as f_in:
                with gzip.open(compressed_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            os.remove(filename)
            return compressed_name
        except Exception as e:
            self.run_logger.error(f"Compress log_file failed: {e}")
            return filename

    def _manage_backups(self):
        try:
            backups = [f for f in os.listdir(self.base_dir)
                       if f.endswith('.gz') and "Oracle" in f]

            backups.sort(key=lambda x: os.path.getmtime(os.path.join(self.base_dir, x)))

            while len(backups) > self.backup_count:
                oldest = backups.pop(0)
                os.remove(os.path.join(self.base_dir, oldest))
                self.run_logger.info(f"Remove old log file: {oldest}")
        except Exception as e:
            self.run_logger.error(f"Manage backup log failed: {e}")

class DataLogger(threading.Thread):
    def __init__(
        self,
        db_info,
        message_queue,
        interval,
    ):
        super().__init__()
        self.db_info = db_info
        self.log_queue = message_queue
        self.interval = interval
        self.record_counter = 0
        self.print_len = None
        self.log_title = None
        self.running = True
        self.run_logger = Logger(name='data', log_file='dbtop_app.log')
        self.logger = logging.getLogger("data_logger")
        self.logger.setLevel(logging.INFO)

        handler = CompressedDynamicFileHandler(
            db_info,
            base_dir = Config.get("main.persist_file_base_dir"),
            max_bytes = Config.get("main.persist_file_max_size") * 1024 * 1024,
            backup_count = Config.get("main.max_backup_count")
        )

        formatter = logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def run(self):
        while self.running:
            try:
                log_line = []
                ins_record = None
                db_record = None
                os_record = None
                while ins_record is None or db_record is None or os_record is None:
                    try:
                        record = self.log_queue.get(timeout=0.5)
                        if record[0] == 'ins' and ins_record is None:
                            ins_record = list(zip(*record[1]))
                        if record[0] == 'db' and db_record is None:
                            db_record = list(zip(*record[1]))
                        if record[0] == 'os' and os_record is None:
                            os_record = list(zip(*record[1]))
                    except queue.Empty:
                        pass
                self.log_queue.queue.clear()
                log_line.extend(os_record[1])
                log_line.extend(db_record[1])
                log_line.extend(ins_record[1])
                log_line = list(map(str, log_line))

                if self.print_len is None:
                    self.log_title = []
                    self.log_title.extend(os_record[0])
                    self.log_title.extend(db_record[0])
                    self.log_title.extend(ins_record[0])

                    self.print_len = []
                    self.print_len.extend(os_record[2])
                    self.print_len.extend(db_record[2])
                    self.print_len.extend(ins_record[2])

                    for i, (length, element) in enumerate(zip(self.print_len, self.log_title)):
                        self.log_title[i] = element.ljust(length)

                if self.record_counter % 30 == 0:
                    self.record_counter = 0
                    self.logger.info('|'.join(self.log_title))

                for i, (length, element) in enumerate(zip(self.print_len, log_line)):
                    log_line[i] = str(element).ljust(length)
                self.logger.info('|'.join(log_line))
                self.record_counter += 1
            except queue.Empty:
                continue
            except Exception as e:
                self.run_logger.error(f"Handle log error: {e}")
            sleep(self.interval)

    def log(self, level, message):
        record = self.logger.makeRecord(
            self.logger.name,
            level,
            "",
            0,
            message,
            None,
            None
        )
        self.log_queue.put(record)

    def stop(self):
        self.run_logger.error("The data logger thread is starting to exit.")
        self.running = False
        self.join()
        self.run_logger.error("The data logger thread has exited.")
