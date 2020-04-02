#!/usr/bin/env python3.7
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

##
## Generate a yml from env.py
##
## ./gen-log-from-env.py <template yml file> [<template yml file>]
##

import collections
import os
import copy
import sys
import yaml

LOG_ENV_PREFIX = 'LOG_'

if len(sys.argv) < 2:
    print('Usage: %s' % (sys.argv[0]))
    sys.exit(1)

conf_files = sys.argv[1:]

def modify(conf, key_parts):
    if "Root" not in key_parts:
        return
    stack = [[conf, 1]]
    while stack:
        root, start = stack.pop()
        for i, component in enumerate(key_parts[start:], start):
            if isinstance(root, collections.Sequence):
                for child in root:
                    stack.append([child, i])
                break
            else:
                if i == len(key_parts) - 1:
                    root[component] = v
                else:
                    root = root[component]

def append(conf, key_parts, added_path):
    if "Logger" not in key_parts:
        return
    root = conf
    for i, component in enumerate(key_parts[1:], 1):
        if (isinstance(root, collections.Sequence) and
                not isinstance(root, str)):
            path = "_".join(key_parts[:i])
            if path in added_path:
                root = root[-1]
            elif key_parts[i-1] == "Logger":
                new = copy.copy(root[-1])
                root.append(new)
                added_path.add(path)
                root = new
            else:
                root = root[-1]

        if i == len(key_parts) - 1:
            root[component] = v
        else:
            root = root[component]

for conf_filename in conf_files:
    conf = yaml.load(open(conf_filename))

    # update the config
    modified = False
    add_logger = {}
    added_path = set()

    for k in sorted(os.environ.keys()):
        if not k.startswith(LOG_ENV_PREFIX):
            continue
        v = os.environ[k]
        key_parts = k.split('_')

        modify(conf, key_parts)
        append(conf, key_parts, added_path)

    # Store back the updated config in the same file
    noalias_dumper = yaml.dumper.SafeDumper
    noalias_dumper.ignore_aliases = lambda self, data: True
    f = open(conf_filename, 'w')
    yaml.dump(conf, f, default_flow_style=False, Dumper=noalias_dumper)
    f.close()
