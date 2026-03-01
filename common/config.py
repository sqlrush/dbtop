# -*- coding: utf-8 -*-
# Copyright (c) ailinkdb. All rights reserved.
# Author: sqlrush

import os
from copy import deepcopy
import threading
from common.log import Logger
import configparser

MAPPING = {
    'interval': 'main.interval',
    'log_interval': 'main.log_interval',
    'user': 'main.user',
    'port': 'main.port',
    'host': 'main.host',
    'service_name': 'main.service_name',
    'daemon': 'main.daemon'
}

class Config:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if not cls._instance:
                cls._instance = super(Config, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._load_config()
            self._initialized = True

    def __init__(self, global_config_path, args):
        self.logger = Logger(__name__)
        self.global_config_path = global_config_path
        self.global_config = self._load_config(self.global_config_path)
        self.merged_config = self._merge_args(vars(args))
        self.config = self._post_process(self.merged_config)
        self.initialized = True

    def _merge_configs(self):
        def merge(a, b):
            result = deepcopy(a)
            for key, value in b.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = merge(result[key], value)
                else:
                    result[key] = value
            return result
        merged_config = merge(self.module_config, self.global_config)
        self.logger.debug("Successfully merged global and module configurations.")
        return merged_config

    def _merge_args(self, args_dict):
        config = deepcopy(self.global_config)

        for arg_name, config_path in MAPPING.items():
            if arg_name in args_dict and args_dict[arg_name] is not None:
                keys = config_path.split('.')
                current = config
                for key in keys[:-1]:
                    if key not in current:
                        current[key] = {}
                    current = current[key]
                current[keys[-1]] = args_dict[arg_name]
        return config

    def _parse_value(self, value: str):
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            content = value[1:-1]
            return content
        try:
            if value.isdigit():
                return int(value)
            if '.' in value and not (value.startswith('.') or value.endswith('.')) and value.replace('.', '').isdigit():
                return float(value)
            if value.lower() in ('true', 'false'):
                return value.lower() == 'true'
            return value
        except ValueError:
            self.logger.error(f"can't parse configuration value '{value}'.")
            raise ValueError(f"can't parse configuration value '{value}'.")

    def _load_config(self, config_path):
        if not os.path.exists(config_path):
            self.logger.error(f"Configuration file not found: {config_path}")
            return {}
        config = configparser.ConfigParser()
        config.read(config_path)
        root = {}
        for section_name in config.sections():
            keys = section_name.split('.')
            current = root
            for i, key in enumerate(keys):
                if i == len(keys) - 1:
                    config_item = {}
                    for real_key, real_value in config[section_name].items():
                        config_item[real_key] = self._parse_value(real_value)
                    current[key] = config_item
                else:
                    if key not in current:
                        current[key] = {}
                    current = current[key]
        return root

    @staticmethod
    def _post_process(config):
        main_dict = config.get("main", {})
        interval = main_dict.get("interval")
        log_interval = main_dict.get("log_interval")
        if interval is not None and log_interval is not None:
            if log_interval > 0:
                main_dict["interval"] = min(interval, log_interval)
        return config

    @classmethod
    def init_instance(cls, global_config_path, args=None):
        if not cls._instance:
            cls._instance = cls(global_config_path, args)
        return cls._instance

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            raise RuntimeError("Config instance not initialized. Please call init_instance first.")
        return cls._instance

    @classmethod
    def get(cls, key):
        instance = cls.get_instance()
        keys = key.split(".")
        config = instance.config
        for k in keys:
            if isinstance(config, dict) and k in config:
                config = config[k]
            else:
                return None
        return config

    @classmethod
    def set(cls, key, value):
        instance = cls.get_instance()
        keys = key.split(".")
        config = instance.config
        if isinstance(config, dict) and keys[0] in config:
            config[keys[0]][keys[1]] = value
        return config
