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

ROOT_DIR=$(git rev-parse --show-toplevel)
container_id=$(docker ps | grep pulsar-website-nginx | awk '{print $1}')

if [ -n "$container_id" ]
then
docker rm -f $container_id
fi

docker run --name pulsar-website-nginx -p 80:80 -v $ROOT_DIR/generated-site/content:/usr/share/nginx/html:ro -d nginx

echo "Website is running: http://localhost"
