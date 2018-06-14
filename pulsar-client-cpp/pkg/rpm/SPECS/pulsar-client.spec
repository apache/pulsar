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

%define name        pulsar-client
%define release     1

%define buildroot   %{_topdir}/%{name}-%{version}-root

BuildRoot:  %{buildroot}
Summary:        Apache Pulsar client library
License:        Apache License v2
Name:           %{name}
Version:        %{version}
Release:        %{release}
Source:         apache-pulsar-%{pom_version}-src.tar.gz
Prefix:         /usr

%description
The Apache Pulsar client contains a C++ and C APIs to interact
with Apache Pulsar brokers.

%prep
%setup -q -n apache-pulsar-%{pom_version}

%build
cd pulsar-client-cpp
cmake . -DBUILD_TESTS=OFF -DLINK_STATIC=ON
make pulsarShared pulsarStatic -j 3

%install
cd pulsar-client-cpp
INCLUDE_DIR=$RPM_BUILD_ROOT/usr/include
LIB_DIR=$RPM_BUILD_ROOT/usr/lib
mkdir -p $INCLUDE_DIR $LIB_DIR
cp -ar include/pulsar $INCLUDE_DIR
cp lib/libpulsar* $LIB_DIR

%files
%defattr(-,root,root)
/usr/lib/libpulsar.a
/usr/lib/libpulsar.so
/usr/lib/libpulsar.so.%{pom_version}
/usr/include/pulsar

# doc %attr(0444,root,root) /usr/local/share/man/man1/wget.1
