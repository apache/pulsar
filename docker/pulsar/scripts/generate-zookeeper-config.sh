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

# Apply env variables to config file and start the regular command

CONF_FILE=$1

if [ $? != 0 ]; then
    echo "Error: Failed to apply changes to config file"
    exit 1
fi

DOMAIN=`hostname -d`

# Generate list of servers and detect the current server ID,
# based on the hostname
IDX=1
for SERVER in $(echo $ZOOKEEPER_SERVERS | tr "," "\n")
do
    echo "server.$IDX=$SERVER.$DOMAIN:2888:3888" >> $CONF_FILE

    if [ "$HOSTNAME" == "$SERVER" ]; then
        MY_ID=$IDX
        echo "Current server id $MY_ID"
    fi

	((IDX++))
done

# For ZooKeeper container we need to initialize the ZK id
if [ ! -z "$MY_ID" ]; then
    # Get ZK data dir
    DATA_DIR=`grep '^dataDir=' $CONF_FILE | awk -F= '{print $2}'`
    if [ ! -e $DATA_DIR/myid ]; then
        echo "Creating $DATA_DIR/myid with id = $MY_ID"
        mkdir -p $DATA_DIR
        echo $MY_ID > $DATA_DIR/myid
    fi
fi
