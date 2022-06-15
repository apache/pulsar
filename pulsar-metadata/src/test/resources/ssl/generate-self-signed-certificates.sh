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

# Copied from https://github.com/etcd-io/jetcd/blob/ff7d698b046367d243a8d9d5cfe528f9bb0e933f/jetcd-core/src/test/resources/ssl/generate-self-signed-certificates.sh

ROOT="$(cd "$(dirname $0)" && pwd)"

CFSSL_HOME=${ROOT}/cfssl
CERT_HOME=${ROOT}/cert

mkdir -p $CFSSL_HOME
mkdir -p $CERT_HOME

OS="$(uname -s)"
case $OS in
    "Linux")
      PLATFORM="linux_amd64"
    ;;
    "Darwin")
      PLATFORM="darwin_amd64"
    ;;
esac

curl -L https://github.com/cloudflare/cfssl/releases/download/v1.6.1/cfssl_1.6.1_${PLATFORM}> cfssl/cfssl
curl -L https://github.com/cloudflare/cfssl/releases/download/v1.6.1/cfssljson_1.6.1_${PLATFORM}> cfssl/cfssljson
chmod +x cfssl/{cfssl,cfssljson}

cd $CERT_HOME

echo '{"CN":"CA","key":{"algo":"rsa","size":2048}}' | $CFSSL_HOME/cfssl gencert -initca - | $CFSSL_HOME/cfssljson -bare ca -
echo '{"signing":{"default":{"expiry":"876000h","usages":["signing","key encipherment","server auth","client auth"]}}}' > ca-config.json

export ADDRESS=etcd-ssl
export NAME=server
echo '{"CN":"'$NAME'","hosts":[""],"key":{"algo":"rsa","size":2048}}' | $CFSSL_HOME/cfssl gencert -config=ca-config.json -ca=ca.pem -ca-key=ca-key.pem -hostname="$ADDRESS" - | $CFSSL_HOME/cfssljson -bare $NAME

export ADDRESS=etcd0
export NAME=etcd0
echo '{"CN":"'$NAME'","hosts":[""],"key":{"algo":"rsa","size":2048}}' | $CFSSL_HOME/cfssl gencert -config=ca-config.json -ca=ca.pem -ca-key=ca-key.pem -hostname="$ADDRESS" - | $CFSSL_HOME/cfssljson -bare $NAME

export ADDRESS=etcd1
export NAME=etcd1
echo '{"CN":"'$NAME'","hosts":[""],"key":{"algo":"rsa","size":2048}}' | $CFSSL_HOME/cfssl gencert -config=ca-config.json -ca=ca.pem -ca-key=ca-key.pem -hostname="$ADDRESS" - | $CFSSL_HOME/cfssljson -bare $NAME

export ADDRESS=etcd2
export NAME=etcd2
echo '{"CN":"'$NAME'","hosts":[""],"key":{"algo":"rsa","size":2048}}' | $CFSSL_HOME/cfssl gencert -config=ca-config.json -ca=ca.pem -ca-key=ca-key.pem -hostname="$ADDRESS" - | $CFSSL_HOME/cfssljson -bare $NAME

export ADDRESS=
export NAME=client
echo '{"CN":"'$NAME'","hosts":[""],"key":{"algo":"rsa","size":2048}}' | $CFSSL_HOME/cfssl gencert -config=ca-config.json -ca=ca.pem -ca-key=ca-key.pem -hostname="$ADDRESS" - | $CFSSL_HOME/cfssljson -bare $NAME

openssl pkcs8 -topk8 -inform PEM -outform PEM -in client-key.pem -out client-key-pk8.pem -nocrypt
