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
## export prefix_my-key=new-value
## ./apply-config-from-env-with-prefix prefix_ file.conf
##

import os, sys

if len(sys.argv) < 3:
    print('Usage: %s' % (sys.argv[0]))
    sys.exit(1)

# Always apply env config to env scripts as well
prefix = sys.argv[1]
conf_files = sys.argv[2:]

PF_ENV_DEBUG = (os.environ.get('PF_ENV_DEBUG','0') == '1')

for conf_filename in conf_files:
    lines = []  # List of config file lines
    keys = {} # Map a key to its line number in the file

    # Load conf file
    for line in open(conf_filename):
        lines.append(line)
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        try:
            k,v = line.split('=', 1)
            keys[k] = len(lines) - 1
        except:
            if PF_ENV_DEBUG:
                print("[%s] skip Processing %s" % (conf_filename, line))

    # Update values from Env
    for k in sorted(os.environ.keys()):
        v = os.environ[k].strip()

        # Hide the value in logs if is password.
        if "password" in k:
            displayValue = "********"
        else:
            displayValue = v

        if k.startswith(prefix):
            k = k[len(prefix):]
        if k in keys:
            print('[%s] Applying config %s = %s' % (conf_filename, k, displayValue))
            idx = keys[k]
            lines[idx] = '%s=%s\n' % (k, v)


    # Add new keys from Env
    for k in sorted(os.environ.keys()):
        v = os.environ[k]
        if not k.startswith(prefix):
            continue

        # Hide the value in logs if is password.
        if "password" in k:
            displayValue = "********"
        else:
            displayValue = v

        k = k[len(prefix):]
        if k not in keys:
            print('[%s] Adding config %s = %s' % (conf_filename, k, displayValue))
            lines.append('%s=%s\n' % (k, v))
        else:
            print('[%s] Updating config %s = %s' % (conf_filename, k, displayValue))
            lines[keys[k]] = '%s=%s\n' % (k, v)


    # Store back the updated config in the same file
    f = open(conf_filename, 'w')
    for line in lines:
        f.write(line)
    f.close()

