#!/usr/bin/env bash
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

############################################################
# Edit a properties config file and replace values based on
# the ENV variables
# export prefix_my-key=new-value
# ./apply-config-from-env-with-prefix prefix_ file.conf
#
# Environment variables that are prefixed with the command
# line prefix will be used to updated file properties if
# they exist and create new ones if they don't.
#
# Environment variables not prefixed will be used only to
# update if they exist and ignored if they don't.
############################################################

# DEPRECATED: Use "apply-config-from-env.py --prefix MY_PREFIX_ conf_file" instead

# this is not a python script, but a bash script. Call apply-config-from-env.py with the prefix argument
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
"${SCRIPT_DIR}/apply-config-from-env.py" --prefix "$1" "${@:2}"
