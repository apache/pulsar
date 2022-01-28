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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. >/dev/null 2>&1 && pwd)"

cd /tmp
mkdir keygendir$$
cd keygendir$$

# create CA key and cert
function generate_ca() {
  openssl req -x509 -nodes -newkey rsa:2048 -keyout ca-key -outform pem -text -out ca-cert.pem -days 3650 -sha256 \
    -subj "/CN=CARoot" -extensions v3_ca
}

function reissue_certificate() {
  keyfile=$1
  certfile=$2
  openssl x509 -x509toreq -in $certfile -signkey $keyfile -out ${certfile}.csr
  openssl x509 -req -CA ca-cert.pem -CAkey ca-key -in ${certfile}.csr -text -outform pem -out $certfile -days 3650 -CAcreateserial
}

generate_ca
cp ca-cert.pem $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/cacert.pem
reissue_certificate $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/client-key.pem \
  $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/client-cert.pem
reissue_certificate $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/server-key.pem \
  $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/server-cert.pem

generate_ca
cp ca-cert.pem $ROOT_DIR/bouncy-castle/bcfips-include-test/src/test/resources/authentication/tls/cacert.pem
reissue_certificate $ROOT_DIR/bouncy-castle/bcfips-include-test/src/test/resources/authentication/tls/broker-key.pem \
  $ROOT_DIR/bouncy-castle/bcfips-include-test/src/test/resources/authentication/tls/broker-cert.pem
reissue_certificate $ROOT_DIR/bouncy-castle/bcfips-include-test/src/test/resources/authentication/tls/client-key.pem \
  $ROOT_DIR/bouncy-castle/bcfips-include-test/src/test/resources/authentication/tls/client-cert.pem

generate_ca
cp ca-cert.pem $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/broker-cacert.pem
reissue_certificate $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/broker-key.pem \
  $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/broker-cert.pem

generate_ca
cp ca-cert.pem $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-cacert.pem
reissue_certificate $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-key.pem \
  $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/client-cert.pem

generate_ca
cp ca-cert.pem $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/proxy-cacert.pem
reissue_certificate $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/proxy-key.pem \
  $ROOT_DIR/pulsar-proxy/src/test/resources/authentication/tls/ProxyWithAuthorizationTest/proxy-cert.pem




cd $ROOT_DIR
rm -rf /tmp/keygendir$$
