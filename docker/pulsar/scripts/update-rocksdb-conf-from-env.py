#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# allows tuning of RocksDB configuration via environment variables which were effective
# before Pulsar 2.11 / BookKeeper 4.15 / https://github.com/apache/bookkeeper/pull/3056
# the script should be applied to the `conf/entry_location_rocksdb.conf` file

import os
import sys
import configparser

# Constants for section keys
DB_OPTIONS = "DBOptions"
CF_OPTIONS = "CFOptions \"default\""
TABLE_OPTIONS = "TableOptions/BlockBasedTable \"default\""

def update_ini_file(ini_file_path):
    config = configparser.ConfigParser()
    config.read(ini_file_path)
    updated = False

    # Mapping of environment variables to INI sections and keys
    env_to_ini_mapping = {
        "dbStorage_rocksDB_logPath": (DB_OPTIONS, "log_path"),
        "dbStorage_rocksDB_logLevel": (DB_OPTIONS, "info_log_level"),
        "dbStorage_rocksDB_lz4CompressionEnabled": (CF_OPTIONS, "compression"),
        "dbStorage_rocksDB_writeBufferSizeMB": (CF_OPTIONS, "write_buffer_size"),
        "dbStorage_rocksDB_sstSizeInMB": (CF_OPTIONS, "target_file_size_base"),
        "dbStorage_rocksDB_blockSize": (TABLE_OPTIONS, "block_size"),
        "dbStorage_rocksDB_bloomFilterBitsPerKey": (TABLE_OPTIONS, "filter_policy"),
        "dbStorage_rocksDB_blockCacheSize": (TABLE_OPTIONS, "block_cache"),
        "dbStorage_rocksDB_numLevels": (CF_OPTIONS, "num_levels"),
        "dbStorage_rocksDB_numFilesInLevel0": (CF_OPTIONS, "level0_file_num_compaction_trigger"),
        "dbStorage_rocksDB_maxSizeInLevel1MB": (CF_OPTIONS, "max_bytes_for_level_base"),
        "dbStorage_rocksDB_format_version": (TABLE_OPTIONS, "format_version")
    }

    # Type conversion functions
    def mb_to_bytes(mb):
        return str(int(mb) * 1024 * 1024)

    def str_to_bool(value):
        return True if value.lower() in ["true", "1", "yes"] else False

    # Iterate over environment variables
    for key, value in os.environ.items():
        if key.startswith("PULSAR_PREFIX_"):
            key = key[len("PULSAR_PREFIX_"):]

        if key in env_to_ini_mapping:
            section, option = env_to_ini_mapping[key]
            if key in ["dbStorage_rocksDB_writeBufferSizeMB", "dbStorage_rocksDB_sstSizeInMB", "dbStorage_rocksDB_maxSizeInLevel1MB"]:
                value = mb_to_bytes(value)
            elif key == "dbStorage_rocksDB_lz4CompressionEnabled":
                value = "kLZ4Compression" if str_to_bool(value) else "kNoCompression"
            elif key == "dbStorage_rocksDB_bloomFilterBitsPerKey":
                value = "rocksdb.BloomFilter:{}:false".format(value)
            if config.get(section, option, fallback=None) != value:
                config.set(section, option, value)
                updated = True

    # Write the updated INI file only if there were updates
    if updated:
        with open(ini_file_path, 'w') as configfile:
            config.write(configfile)

if __name__ == "__main__":
    ini_file_path = sys.argv[1] if len(sys.argv) > 1 else "conf/entry_location_rocksdb.conf"
    update_ini_file(ini_file_path)