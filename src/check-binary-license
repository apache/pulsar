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

# Script to check licenses on a binary tarball.
# It extracts the list of bundled jars, the NOTICE, and the LICENSE
# files. It checked that every non-pulsar jar bundled is mentioned in the
# LICENSE file. It checked that all jar files mentioned in NOTICE and
# LICENSE are actually bundled.

# all error fatal
set -e

# skip checks for Presto licenses if 1. argument is "--no-presto"/"no-pulsar-sql"
# this is to allow building the server distribution without Pulsar SQL
NO_PRESTO=0
if [[ "$1" == "--no-presto" || "$1" == "--no-pulsar-sql" ]]; then
  NO_PRESTO=1
  shift
fi

TARBALL="$1"
if [ -z $TARBALL ]; then
    echo "Usage: $0 <binary-tarball>"
    exit 1
fi

JARS=$(tar -tf $TARBALL | grep '\.jar' | grep -v 'lib/presto/' | grep -v '/examples/' | grep -v '/instances/' | sed 's!.*/!!' | sort)

LICENSEPATH=$(tar -tf $TARBALL  | awk '/^[^\/]*\/LICENSE/')
LICENSE=$(tar -O -xf $TARBALL "$LICENSEPATH")
NOTICEPATH=$(tar -tf $TARBALL  | awk '/^[^\/]*\/NOTICE/')
NOTICE=$(tar -O -xf $TARBALL $NOTICEPATH)

LICENSEJARS=$(echo "$LICENSE" | sed -nE 's!.* (.*\.jar).*!\1!gp')
NOTICEJARS=$(echo "$NOTICE" | sed -nE 's!.* (.*\.jar).*!\1!gp')

LINKEDINLICENSE=$(echo "$LICENSE" | sed -nE 's!.*(lib/[[:graph:]]*).*!\1!gp' | sed 's!\.$!!')

# errors not fatal
set +e

EXIT=0


# Check all bundled jars are mentioned in LICENSE
for J in $JARS; do
    echo $J | grep -q "org.apache.pulsar"
    if [ $? == 0 ]; then
        continue
    fi

    echo "$LICENSE" | grep -q $J
    if [ $? != 0 ]; then
        echo $J unaccounted for in LICENSE
        EXIT=1
    fi
done

# Check all jars mentioned in LICENSE are bundled
for J in $LICENSEJARS; do
    echo "$JARS" | grep -q $J
    if [ $? != 0 ]; then
        echo $J mentioned in LICENSE, but not bundled
        EXIT=2
    fi
done

# Check all jars mentioned in NOTICE are bundled
for J in $NOTICEJARS; do
    if [ $J == "checker-qual.jar" ]; then
        continue
    fi
    echo "$JARS" | grep -q $J
    if [ $? != 0 ]; then
        echo $J mentioned in NOTICE, but not bundled
        EXIT=3
    fi
done

if [ "$NO_PRESTO" -ne 1 ]; then
  # check pulsar sql jars
  JARS=$(tar -tf $TARBALL | grep '\.jar' | grep 'lib/presto/' | grep -v pulsar-client | grep -v bouncy-castle-bc | grep -v pulsar-metadata | grep -v 'managed-ledger' | grep -v  'pulsar-client-admin' | grep -v  'pulsar-client-api' | grep -v 'pulsar-functions-api' | grep -v 'pulsar-presto-connector-original' | grep -v 'pulsar-presto-distribution' | grep -v 'pulsar-common' | grep -v 'pulsar-functions-proto' | grep -v 'pulsar-functions-utils' | grep -v 'pulsar-io-core' | grep -v 'pulsar-transaction-common' | grep -v 'pulsar-package-core' | grep -v 'java-version-trim-agent' | sed 's!.*/!!' | sort)
  LICENSEPATH=$(tar -tf $TARBALL  | awk '/^[^\/]*\/lib\/presto\/LICENSE/')
  LICENSE=$(tar -O -xf $TARBALL "$LICENSEPATH")
  LICENSEJARS=$(echo "$LICENSE" | sed -nE 's!.* (.*\.jar).*!\1!gp')


  for J in $JARS; do
      echo $J | grep -q "org.apache.pulsar"
      if [ $? == 0 ]; then
          continue
      fi

      echo "$LICENSE" | grep -q $J
      if [ $? != 0 ]; then
          echo $J unaccounted for in lib/presto/LICENSE
          EXIT=1
      fi
  done

  # Check all jars mentioned in LICENSE are bundled
  for J in $LICENSEJARS; do
      echo "$JARS" | grep -q $J
      if [ $? != 0 ]; then
          echo $J mentioned in lib/presto/LICENSE, but not bundled
          EXIT=2
      fi
  done
fi

if [ $EXIT != 0 ]; then
    echo
    echo It looks like there are issues with the LICENSE/NOTICE.
fi

exit $EXIT

