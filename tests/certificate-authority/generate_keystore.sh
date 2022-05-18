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

rm -rf jks
mkdir jks
cd jks

DAYS=36500

COMMON_PARAMS="-storetype JKS -storepass 111111 -keypass 111111 -noprompt"

# generate keystore
keytool -genkeypair -keystore broker.keystore.jks $COMMON_PARAMS -keyalg RSA -keysize 2048 -alias broker -validity $DAYS \
  -dname 'CN=localhost,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown'
keytool -genkeypair -keystore client.keystore.jks $COMMON_PARAMS -keyalg RSA -keysize 2048 -alias client -validity $DAYS \
  -dname 'CN=clientuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown'

# export certificate
keytool -exportcert -keystore broker.keystore.jks $COMMON_PARAMS -file broker.cer -alias broker
keytool -exportcert -keystore client.keystore.jks $COMMON_PARAMS -file client.cer -alias client

# generate truststore
keytool -importcert -keystore client.truststore.jks $COMMON_PARAMS -file client.cer -alias truststore
keytool -importcert -keystore broker.truststore.jks $COMMON_PARAMS -file broker.cer -alias truststore

# generate a truststore without password
java ../RemoveJksPassword.java client.truststore.jks 111111 client.truststore.nopassword.jks
java ../RemoveJksPassword.java broker.truststore.jks 111111 broker.truststore.nopassword.jks

# cleanup
rm broker.cer
rm client.cer
