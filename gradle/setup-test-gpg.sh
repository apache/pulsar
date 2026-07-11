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

# Creates a temporary GPG keyring for testing artifact signing.
# Does NOT touch ~/.gnupg — uses an isolated keyring in build/tmp-gpg-home.
#
# Usage:
#   source gradle/setup-test-gpg.sh
#   ./gradlew publishAllPublicationsToLocalDeployRepository \
#     -PuseGpgCmd=true \
#     -Psigning.gnupg.keyName=$TEST_GPG_KEY_ID \
#     -Psigning.gnupg.homeDir=$GNUPGHOME

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

export GNUPGHOME="$PROJECT_DIR/build/tmp-gpg-home"
mkdir -p "$GNUPGHOME"
chmod 700 "$GNUPGHOME"

echo "Generating temporary GPG key in $GNUPGHOME ..."
gpg --homedir "$GNUPGHOME" --batch --gen-key <<EOF
%no-protection
Key-Type: RSA
Key-Length: 2048
Name-Real: Test Signing Key
Name-Email: test@example.com
Expire-Date: 0
%commit
EOF

TEST_GPG_KEY_ID=$(gpg --homedir "$GNUPGHOME" --list-keys --keyid-format long 2>/dev/null \
    | grep -E '^\s+[0-9A-F]+' | head -1 | awk '{print $1}')

export TEST_GPG_KEY_ID

echo ""
echo "GPG key created: $TEST_GPG_KEY_ID"
echo "GNUPGHOME=$GNUPGHOME"
echo ""
echo "To publish with signing:"
echo "  ./gradlew publishAllPublicationsToLocalDeployRepository \\"
echo "    -PuseGpgCmd=true \\"
echo "    -Psigning.gnupg.keyName=$TEST_GPG_KEY_ID \\"
echo "    -Psigning.gnupg.homeDir=$GNUPGHOME"
