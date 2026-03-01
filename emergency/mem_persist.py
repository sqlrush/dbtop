# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

from common import log
from common.config import Config


class MemPersist:
    def __init__(self):
        # create logger
        self.logger = log.Logger(name='persist', log_file='dbtop_emergency_run.log', level='INFO')
        # init mem
        self.snap_id = 0
        self.ins_info_snap_dict = dict() # key: snap_id  value: ins_info
        self.sql_info_snap_dict = dict() # key: snap_id  value: sql_info_array
        self.emergency_sql_info_snap_dict = dict() # key: snap_id  value: emergency_sql_info_array
        self.max_snap_num = Config.get("emergency.plan_change.max_sql_snapshot_number")

    def persist_ins_info(self, ins_info):
        if len(self.ins_info_snap_dict) == self.max_snap_num:
            min_key = min(self.ins_info_snap_dict.keys())
            del self.ins_info_snap_dict[min_key]

        snap_id = ins_info["snap_id"]
        if snap_id in self.ins_info_snap_dict.keys():
            self.logger.error("duplicate snap_id in ins_info_snap_dict: %d", snap_id)
        else:
            self.ins_info_snap_dict[snap_id] = ins_info
        return

    def persist_sql_info(self, sql_info):
        if len(self.sql_info_snap_dict) == self.max_snap_num:
            min_key = min(self.sql_info_snap_dict.keys())
            del self.sql_info_snap_dict[min_key]

        snap_id = sql_info["snap_id"]
        if snap_id in self.sql_info_snap_dict.keys():
            sql_info_array = self.sql_info_snap_dict[snap_id]
            sql_info_array.append(sql_info)
        else:
            self.sql_info_snap_dict[snap_id] = [ sql_info ]
        return

    def persist_emergency_sql_info(self, sql_info):
        if len(self.emergency_sql_info_snap_dict) == self.max_snap_num:
            min_key = min(self.emergency_sql_info_snap_dict.keys())
            del self.emergency_sql_info_snap_dict[min_key]

        snap_id = sql_info["snap_id"]
        if snap_id in self.emergency_sql_info_snap_dict.keys():
            emergency_sql_info_array = self.emergency_sql_info_snap_dict[snap_id]
            emergency_sql_info_array.append(sql_info)
        else:
            self.emergency_sql_info_snap_dict[snap_id] = [ sql_info ]
        return

    def get_snap_id(self):
        self.snap_id += 1
        return [ [ self.snap_id ] ]

    def get_sql_info_snap(self, db_id, target_snap_id, unique_sql_id):
        sql_info_snap_array = []
        for snap_id in sorted(self.sql_info_snap_dict.keys()):
            sql_info_snap = []
            if snap_id < target_snap_id - Config.get("emergency.plan_change.snapshot_compare_scope") or snap_id > target_snap_id - 1:
                continue

            sql_info_array = self.sql_info_snap_dict[snap_id]
            for sql_info in sql_info_array:
                if sql_info["unique_sql_id"] != unique_sql_id:
                    continue

                for key, value in sql_info.items():
                    sql_info_snap.append(value)

            if len(sql_info_snap) != 0:
                sql_info_snap_array.append(sql_info_snap)

        if len(sql_info_snap_array) == 0:
            return None

        return sql_info_snap_array

    def get_ins_info_snap(self, db_id, snap_id):
        ins_info_snap = []
        if snap_id in self.ins_info_snap_dict.keys():
            ins_info = self.ins_info_snap_dict[snap_id]
            for key, value in ins_info.items():
                ins_info_snap.append(value)

        if len(ins_info_snap) == 0:
            return None

        return [ ins_info_snap ]

    def get_emergency_sql_info_snap(self, db_id, unique_sql_id):
        emergency_sql_info_snap = []
        for snap_id, emergency_sql_info_array in self.emergency_sql_info_snap_dict.items():
            for emergency_sql_info in emergency_sql_info_array:
                if emergency_sql_info["unique_sql_id"] == unique_sql_id and emergency_sql_info["recovered"] == False:
                    for key, value in emergency_sql_info.items():
                        emergency_sql_info_snap.append(value)
                    break

        if len(emergency_sql_info_snap) == 0:
            return None

        return [ emergency_sql_info_snap ]

    def get_emergency_sql_unrecovered(self, db_id):
        emergency_sql_info_snap_array = []
        for snap_id, emergency_sql_info_array in self.emergency_sql_info_snap_dict.items():
            for emergency_sql_info in emergency_sql_info_array:
                emergency_sql_info_snap = []
                for key, value in emergency_sql_info.items():
                    emergency_sql_info_snap.append(value)
                emergency_sql_info_snap_array.append(emergency_sql_info_snap)

        return emergency_sql_info_snap_array

    def update_emergency_sql_recovered(self, db_id, snap_id, unique_sql_id):
        if snap_id in self.emergency_sql_info_snap_dict.keys():
            emergency_sql_info_array = self.emergency_sql_info_snap_dict[snap_id]
            new_emergency_sql_info_array = [x for x in emergency_sql_info_array if x["unique_sql_id"] != unique_sql_id]

            if len(new_emergency_sql_info_array) == 0:
                del self.emergency_sql_info_snap_dict[snap_id]
            else:
                self.emergency_sql_info_snap_dict[snap_id] = new_emergency_sql_info_array

        return
