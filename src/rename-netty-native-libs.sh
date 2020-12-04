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

set -e

ARTIFACT_ID=$1
JAR_PATH="$PWD/target/$ARTIFACT_ID.jar"

FILE_PREFIX='META-INF/native'

FILES_TO_RENAME=(
    'libnetty_transport_native_epoll_x86_64.so liborg_apache_pulsar_shade_netty_transport_native_epoll_x86_64.so'
    'libnetty_tcnative_linux_x86_64.so liborg_apache_pulsar_shade_netty_tcnative_linux_x86_64.so'
)

echo "----- Renaming epoll lib in $JAR_PATH ------"
TMP_DIR=`mktemp -d`
CUR_DIR=$(pwd)
cd ${TMP_DIR}
# exclude `META-INF/LICENSE`
unzip -q $JAR_PATH -x "META-INF/LICENSE"
# include `META-INF/LICENSE` as LICENSE.netty.
# This approach is to get around the issue that MacOS is not able to recognize the difference between `META-INF/LICENSE` and `META-INF/license/`.
unzip -p $JAR_PATH META-INF/LICENSE > META-INF/LICENSE.netty
cd ${CUR_DIR}

pushd $TMP_DIR

for line in "${FILES_TO_RENAME[@]}"; do
    read -r -a A <<< "$line"
    FROM=${A[0]}
    TO=${A[1]}

    if [ -f $FILE_PREFIX/$FROM ]; then
        echo "Renaming $FROM -> $TO"
        mv $FILE_PREFIX/$FROM $FILE_PREFIX/$TO
    fi
done

# Overwrite the original ZIP archive
rm $JAR_PATH
zip -q -r $JAR_PATH .
popd

rm -rf $TMP_DIR
