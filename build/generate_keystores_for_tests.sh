#!/bin/bash -xe
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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd /tmp
mkdir keygendir$$
cd keygendir$$

# create CA key and cert
openssl req -x509 -newkey rsa:2048 -passout pass:111111 -keyout ca-key -out ca-cert -days 3650 -sha256 -subj "/CN=CARoot"

COMMON_PARAMS="-storetype JKS -storepass 111111 -keypass 111111 -noprompt"

# create client and broker truststores and keystores
keytool -import -keystore client.truststore.jks $COMMON_PARAMS -alias CARoot -file ca-cert
keytool -import -keystore broker.truststore.jks $COMMON_PARAMS -alias CARoot -file ca-cert
keytool -import -keystore client.keystore.jks $COMMON_PARAMS -alias CARoot -file ca-cert
keytool -import -keystore broker.keystore.jks $COMMON_PARAMS -alias CARoot -file ca-cert

# create broker key
keytool -genkeypair -keystore broker.keystore.jks $COMMON_PARAMS -keyalg RSA -alias localhost -validity 3650 \
  -dname 'CN=localhost,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown'
keytool -certreq -keystore broker.keystore.jks $COMMON_PARAMS -alias localhost -file cert-file
# sign broker key
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 3650 -CAcreateserial -passin pass:111111
# import broker key
keytool -import -keystore broker.keystore.jks $COMMON_PARAMS -alias localhost -file cert-signed

# create client key
keytool -genkeypair -keystore client.keystore.jks $COMMON_PARAMS -keyalg RSA -alias clientuser -validity 3650 \
  -dname 'CN=clientuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown'
keytool  -certreq -keystore client.keystore.jks $COMMON_PARAMS -alias clientuser -file cert-file-client
# sign client key
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file-client -out cert-signed-client -days 3650 -CAcreateserial -passin pass:111111
# import client key
keytool -import -keystore client.keystore.jks $COMMON_PARAMS -alias clientuser -file cert-signed-client

# update keystores used in tests
cp client.truststore.jks broker.truststore.jks client.keystore.jks broker.keystore.jks $SCRIPT_DIR/../pulsar-broker/src/test/resources/authentication/keystoretls/
cp client.truststore.jks broker.truststore.jks client.keystore.jks broker.keystore.jks $SCRIPT_DIR/../pulsar-proxy/src/test/resources/authentication/keystoretls/

cd $SCRIPT_DIR
rm -rf /tmp/keygendir$$
