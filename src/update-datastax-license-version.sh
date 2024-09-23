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
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <filename> <current_version> <new_version>"
  exit 1
fi

FILENAME="$1"
CURRENT_VERSION="$2"
NEXT_VERSION="$3"

echo "$FILENAME"
echo "$CURRENT_VERSION"
echo "$NEXT_VERSION"

if [ ! -f "$FILENAME" ]; then
  echo "Error: File '$FILENAME' not found!"
  exit 1
fi

while IFS= read -r LINE; do
  if [[ "$LINE" == *"$CURRENT_VERSION"* ]]; then
    UPDATED_LINE="${LINE/$CURRENT_VERSION/$NEXT_VERSION}"
    echo "$UPDATED_LINE"
  else
    echo "$LINE"
  fi
done < "$FILENAME" > "$FILENAME.new"

mv "$FILENAME.new" "$FILENAME"
echo "License file '$FILENAME' updated successfully."