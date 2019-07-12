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

%define name        apache-pulsar-client
%define release     1
%define buildroot   %{_topdir}/%{name}-%{version}-root

BuildRoot:      %{buildroot}
Summary:        Apache Pulsar client library
URL:            https://pulsar.apache.org/
License:        Apache License v2
Name:           %{name}
Version:        %{version}
Release:        %{release}
Source:         apache-pulsar-%{pom_version}-src.tar.gz
Prefix:         /usr

%package devel
Summary:        Apache Pulsar client library
Provides:       apache-pulsar-client-devel
Requires:       apache-pulsar-client

%description
The Apache Pulsar client contains a C++ and C APIs to interact
with Apache Pulsar brokers.

%description devel
The Apache Pulsar client contains a C++ and C APIs to interact
with Apache Pulsar brokers.

The devel package contains C++ and C API headers and `libpulsar.a`
static library.

%prep
%setup -q -n apache-pulsar-%{pom_version}

%build
cd pulsar-client-cpp
cmake . -DBUILD_TESTS=OFF -DLINK_STATIC=ON -DBUILD_PYTHON_WRAPPER=OFF
make pulsarShared pulsarStatic -j 3

%install
cd pulsar-client-cpp
INCLUDE_DIR=$RPM_BUILD_ROOT/usr/include
LIB_DIR=$RPM_BUILD_ROOT/usr/lib
DOC_DIR=$RPM_BUILD_ROOT/usr/share/doc/pulsar-client-%{version}
DOC_DEVEL_DIR=$RPM_BUILD_ROOT/usr/share/doc/pulsar-client-devel-%{version}
mkdir -p $INCLUDE_DIR $LIB_DIR $DOC_DIR $DOC_DEVEL_DIR

cp -ar include/pulsar $INCLUDE_DIR
cp lib/libpulsar.a $LIB_DIR
cp lib/libpulsar.so.%{pom_version} $LIB_DIR

# Copy LICENSE files
cp ../NOTICE $DOC_DIR
cp pkg/licenses/* $DOC_DIR

cp $DOC_DIR/* $DOC_DEVEL_DIR/

cd  $LIB_DIR
ln -s libpulsar.so.%{pom_version} libpulsar.so

%files
%defattr(-,root,root)
/usr/lib/libpulsar.so
/usr/lib/libpulsar.so.%{pom_version}
/usr/share/doc/pulsar-client-%{version}

%files devel
%defattr(-,root,root)
/usr/lib/libpulsar.a
/usr/include/pulsar
/usr/share/doc/pulsar-client-devel-%{version}
