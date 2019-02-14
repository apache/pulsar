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

set -e -x

cd /pulsar
SRC_ROOT_DIR=$(git rev-parse --show-toplevel)
cd $SRC_ROOT_DIR/pulsar-client-cpp/pkg/deb

POM_VERSION=`$SRC_ROOT_DIR/src/get-project-version.py`
# Sanitize VERSION by removing `SNAPSHOT` if any since it's not legal in DEB
VERSION=`echo $POM_VERSION | awk -F-  '{print $1}'`

ROOT_DIR=apache-pulsar-$POM_VERSION
CPP_DIR=$ROOT_DIR/pulsar-client-cpp

rm -rf BUILD
mkdir BUILD
cd BUILD
tar xfz $SRC_ROOT_DIR/distribution/server/target/apache-pulsar-$POM_VERSION-src.tar.gz
pushd $CPP_DIR

cmake . -DBUILD_TESTS=OFF -DLINK_STATIC=ON
make pulsarShared pulsarStatic -j 3
popd

DEST_DIR=apache-pulsar-client
mkdir -p $DEST_DIR/DEBIAN
cat <<EOF > $DEST_DIR/DEBIAN/control
Package: apache-pulsar-client
Version: ${VERSION}
Maintainer: Apache Pulsar <dev@pulsar.apache.org>
Architecture: amd64
Description: The Apache Pulsar client contains a C++ and C APIs to interact with Apache Pulsar brokers.
EOF

DEVEL_DEST_DIR=apache-pulsar-client-dev
mkdir -p $DEVEL_DEST_DIR/DEBIAN
cat <<EOF > $DEVEL_DEST_DIR/DEBIAN/control
Package: apache-pulsar-client-dev
Version: ${VERSION}
Maintainer: Apache Pulsar <dev@pulsar.apache.org>
Architecture: amd64
Depends: apache-pulsar-client
Description: The Apache Pulsar client contains a C++ and C APIs to interact with Apache Pulsar brokers.
EOF

mkdir -p $DEST_DIR/usr/lib
mkdir -p $DEVEL_DEST_DIR/usr/lib
mkdir -p $DEVEL_DEST_DIR/usr/include
mkdir -p $DEST_DIR/usr/share/doc/pulsar-client-$VERSION
mkdir -p $DEVEL_DEST_DIR/usr/share/doc/pulsar-client-dev-$VERSION

cp -ar $CPP_DIR/include/pulsar $DEVEL_DEST_DIR/usr/include/
cp $CPP_DIR/lib/libpulsar.a $DEVEL_DEST_DIR/usr/lib
cp $CPP_DIR/lib/libpulsar.so.$POM_VERSION $DEST_DIR/usr/lib
pushd $DEST_DIR/usr/lib
ln -s libpulsar.so.$POM_VERSION libpulsar.so
popd

cp $ROOT_DIR/NOTICE $DEST_DIR/usr/share/doc/pulsar-client-$VERSION
cp $CPP_DIR/pkg/licenses/* $DEST_DIR/usr/share/doc/pulsar-client-$VERSION
cp $CPP_DIR/pkg/licenses/LICENSE.txt $DEST_DIR/usr/share/doc/pulsar-client-$VERSION/copyright
cp $CPP_DIR/pkg/licenses/LICENSE.txt $DEST_DIR/DEBIAN/copyright
cp $CPP_DIR/pkg/licenses/LICENSE.txt $DEVEL_DEST_DIR/DEBIAN/copyright

cp $DEST_DIR/usr/share/doc/pulsar-client-$VERSION/* $DEVEL_DEST_DIR/usr/share/doc/pulsar-client-dev-$VERSION


## Build actual debian packages
dpkg-deb --build $DEST_DIR
dpkg-deb --build $DEVEL_DEST_DIR

mkdir DEB
mv *.deb DEB
cd DEB
dpkg-scanpackages . /dev/null | gzip -9c > Packages.gz
