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

import os
import sys
import configparser
import re

def get_first_word(section_name):
    # Split the section name by any non-word character and return the first word
    return re.split(r'\W+', section_name)[0]

def update_ini_file(ini_file_path, env_prefix):
    # Read the existing INI file
    config = configparser.ConfigParser()
    config.read(ini_file_path)

    # Flag to track if any updates were made
    updated = False

    # Iterate over environment variables
    for key, value in os.environ.items():
        if env_prefix and not key.startswith(env_prefix):
            continue

        stripped_key = key[len(env_prefix):] if env_prefix else key

        # Iterate through sections
        for section in config.sections():
            first_word = get_first_word(section)
            prefix = first_word + '_'
            if stripped_key.startswith(prefix):
                config.set(section, stripped_key[len(prefix):], value)
                updated = True
                break
            elif config.has_option(section, stripped_key):
                config.set(section, stripped_key, value)
                updated = True
                break

    # Write the updated INI file only if there were updates
    if updated:
        with open(ini_file_path, 'w') as configfile:
            config.write(configfile)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 update-ini-from-env.py <path_to_ini_file> <env_prefix>")
        sys.exit(1)

    ini_file_path = sys.argv[1]
    env_prefix = sys.argv[2]
    update_ini_file(ini_file_path, env_prefix)