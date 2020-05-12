#!/bin/bash
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

set -x -e

rm -rf /data/*
chown -R postgres: /data
chmod 700 /data
sudo -u postgres /usr/lib/postgresql/11/bin/initdb /data/
sudo -u postgres /usr/lib/postgresql/11/bin/pg_ctl -D /data/ start
sudo -u postgres psql --command "CREATE USER docker WITH PASSWORD 'docker';"
sudo -u postgres createdb -O docker pulsar_dashboard

cd /pulsar/django
./manage.py migrate
