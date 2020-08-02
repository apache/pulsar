#!/usr/bin/env python
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
## Edit a properties config file and replace values based on
## the ENV variables
## export my-key=new-value
## ./apply-config-from-env-single SQL_PREFIX file.conf
##

import os, sys

print('apply-config-from-evn-single start ...')

if len(sys.argv) < 3:
    print('Usage: %s' % (sys.argv[0]))
    sys.exit(1)

PF_ENV_PREFIX = sys.argv[1]

print('PF_ENV_PREFIX: ' + PF_ENV_PREFIX)

# Always apply env config to env scripts as well
conf_files = sys.argv[2:]

print('confFiles: ' + conf_files)

for conf_filename in conf_files:
    print('conf_filename: ' + conf_filename)
    lines = []  # List of config file lines
    keys = {} # Map a key to its line number in the file

    # Load conf file
    for line in open(conf_filename):
        lines.append(line)
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        k,v = line.split('=', 1)
        keys[k] = len(lines) - 1

    # Update values from Env
    for k in sorted(os.environ.keys()):
        v = os.environ[k]
        if k.startswith(PF_ENV_PREFIX):
            k = k[len(PF_ENV_PREFIX):]
        if k in keys:
            print('[%s] Applying config %s = %s' % (conf_filename, k, v))
            idx = keys[k]
            lines[idx] = '%s=%s\n' % (k, v)


    # Add new keys from Env
    for k in sorted(os.environ.keys()):
        v = os.environ[k]
        if not k.startswith(PF_ENV_PREFIX):
            continue
        k = k[len(PF_ENV_PREFIX):]
        if k not in keys:
            print('[%s] Adding config %s = %s' % (conf_filename, k, v))
            lines.append('%s=%s\n' % (k, v))
        else:
            print('[%s] Updating config %s = %s' %(conf_filename, k, v))
            lines[keys[k]] = '%s=%s\n' % (k, v)

    # Store back the updated config in the same file
    f = open(conf_filename, 'w')
    for line in lines:
        f.write(line)
    f.close()
