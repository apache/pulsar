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

# sets dbStorage_writeCacheMaxSizeMb and dbStorage_readAheadCacheMaxSizeMb if not already defined
export dbStorage_writeCacheMaxSizeMb="${dbStorage_writeCacheMaxSizeMb:-16}"
export dbStorage_readAheadCacheMaxSizeMb="${dbStorage_readAheadCacheMaxSizeMb:-16}"

bin/apply-config-from-env.py conf/bookkeeper.conf && \
    bin/apply-config-from-env.py conf/pulsar_env.sh

if [ -z "$NO_AUTOSTART" ]; then
    sed -i 's/autostart=.*/autostart=true/' /etc/supervisord/conf.d/bookie.conf
fi

bin/watch-znode.py -z $zkServers -p /initialized-$clusterName -w
exec /usr/bin/supervisord -c /etc/supervisord.conf

