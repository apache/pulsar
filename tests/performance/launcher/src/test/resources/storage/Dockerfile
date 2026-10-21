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

# The caller supplies the Alpine-based Pulsar java-test image, not a release base.
ARG PULSAR_IMAGE
FROM ${PULSAR_IMAGE}

USER root
COPY journal-faultfs.c /usr/local/src/journal-faultfs.c
# util-linux supplies mount propagation control and mountpoint consistently on Alpine.
RUN apk add --no-cache fuse3 util-linux \
    && apk add --no-cache --virtual .storage-build fuse3-dev build-base \
    && cc -std=c11 -D_FILE_OFFSET_BITS=64 -O2 -Wall -Wextra -Werror -pthread \
        /usr/local/src/journal-faultfs.c -lfuse3 -o /usr/local/bin/journal-faultfs \
    && apk del .storage-build

RUN test -f /pulsar/bin/run-bookie.sh \
    && test ! -e /pulsar/bin/run-bookie-original.sh \
    && mv /pulsar/bin/run-bookie.sh /pulsar/bin/run-bookie-original.sh
COPY run-bookie.sh /pulsar/bin/run-bookie.sh
RUN chmod 0755 /pulsar/bin/run-bookie.sh /usr/local/bin/journal-faultfs
