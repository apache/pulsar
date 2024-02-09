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
## ./apply-config-from-env file.conf
##

import os, sys, argparse

parser = argparse.ArgumentParser(description='Pulsar configuration file customizer based on environment variables')
parser.add_argument('--prefix', default='PULSAR_PREFIX_', help='Prefix for environment variables, default is PULSAR_PREFIX_')
parser.add_argument('conf_files', nargs='*', help='Configuration files')
args = parser.parse_args()
if not args.conf_files:
    parser.print_help()
    sys.exit(1)

env_prefix = args.prefix
conf_files = args.conf_files

PF_ENV_DEBUG = (os.environ.get('PF_ENV_DEBUG','0') == '1')

# List of keys where the value should not be displayed in logs
sensitive_keys = ["brokerClientAuthenticationParameters", "bookkeeperClientAuthenticationParameters", "tokenSecretKey"]

def sanitize_display_value(k, v):
    if "password" in k.lower() or k in sensitive_keys or (k == "tokenSecretKey" and v.startswith("data:")):
        return "********"
    return v

for conf_filename in conf_files:
    lines = []  # List of config file lines
    keys = {} # Map a key to its line number in the file

    # Load conf file
    for line in open(conf_filename):
        lines.append(line)
        line = line.strip()
        if not line:
            continue
        try:
            k,v = line.split('=', 1)
            if k.startswith('#'):
                k = k[1:]
            keys[k.strip()] = len(lines) - 1
        except:
            if PF_ENV_DEBUG:
                print("[%s] skip Processing %s" % (conf_filename, line))

    # Update values from Env
    for k in sorted(os.environ.keys()):
        v = os.environ[k].strip()

        if k in keys:
            displayValue = sanitize_display_value(k, v)
            print('[%s] Applying config %s = %s' % (conf_filename, k, displayValue))
            idx = keys[k]
            lines[idx] = '%s=%s\n' % (k, v)

    # Ensure we have a new-line at the end of the file, to avoid issue
    # when appending more lines to the config
    lines.append('\n')

    # Add new keys from Env
    for k in sorted(os.environ.keys()):
        if not k.startswith(env_prefix):
            continue

        v = os.environ[k].strip()
        k = k[len(env_prefix):]

        displayValue = sanitize_display_value(k, v)

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