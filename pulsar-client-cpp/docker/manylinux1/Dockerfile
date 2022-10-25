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


FROM quay.io/pypa/manylinux1_x86_64

RUN yum install -y gtest-devel

ARG PYTHON_VERSION
ARG PYTHON_SPEC

ENV PYTHON_VERSION=${PYTHON_VERSION}
ENV PYTHON_SPEC=${PYTHON_SPEC}

ENV PATH="/opt/python/${PYTHON_SPEC}/bin:${PATH}"

RUN ln -s /opt/python/${PYTHON_SPEC}/include/python${PYTHON_VERSION}m /opt/python/${PYTHON_SPEC}/include/python${PYTHON_VERSION}

# Perl (required for building OpenSSL)
RUN curl -O -L https://www.cpan.org/src/5.0/perl-5.10.0.tar.gz && \
    tar xvfz perl-5.10.0.tar.gz && \
    cd perl-5.10.0 && \
    ./configure.gnu --prefix=/usr/local/ && \
    make && make install && \
    rm -rf /perl-5.10.0.tar.gz /perl-5.10.0

####################################
# These dependencies can be found in Ubuntu but they're not compiled with -fPIC,
# so they cannot be statically linked into a shared library
####################################

# ZLib
RUN curl -O -L https://zlib.net/fossils/zlib-1.2.13.tar.gz && \
    tar xvfz zlib-1.2.13.tar.gz && \
    cd zlib-1.2.13 && \
    CFLAGS="-fPIC -O3" ./configure && \
    make && make install && \
    rm -rf /zlib-1.2.13.tar.gz /zlib-1.2.13

# Compile OpenSSL
RUN curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_1_1_1n.tar.gz && \
    tar xvfz OpenSSL_1_1_1n.tar.gz && \
    cd openssl-OpenSSL_1_1_1n/ && \
    ./Configure -fPIC --prefix=/usr/local/ssl/ no-shared linux-x86_64 && \
    make && make install && \
    rm -rf /OpenSSL_1_1_1n.tar.gz /openssl-OpenSSL_1_1_1n

ENV LD_LIBRARY_PATH /usr/local/ssl/lib/:

# Download and compile boost
RUN curl -O -L https://boostorg.jfrog.io/artifactory/main/release/1.68.0/source/boost_1_68_0.tar.gz && \
    tar xvfz boost_1_68_0.tar.gz && \
    cd /boost_1_68_0 && \
    ./bootstrap.sh --with-libraries=program_options,filesystem,regex,thread,system,python && \
    ./b2 address-model=64 cxxflags=-fPIC link=static threading=multi variant=release install && \
    rm -rf /boost_1_68_0.tar.gz /boost_1_68_0

# Download and copile protoubf
RUN curl -O -L  https://github.com/google/protobuf/releases/download/v3.3.0/protobuf-cpp-3.3.0.tar.gz && \
    tar xvfz protobuf-cpp-3.3.0.tar.gz && \
    cd protobuf-3.3.0/ && \
    CXXFLAGS=-fPIC ./configure && \
    make && make install && ldconfig && \
    rm -rf /protobuf-cpp-3.3.0.tar.gz /protobuf-3.3.0

# Compile APR
RUN curl -O -L  http://archive.apache.org/dist/apr/apr-1.5.2.tar.gz && \
    tar xvfz apr-1.5.2.tar.gz && \
    cd apr-1.5.2 && \
    CFLAGS=-fPIC CXXFLAGS=-fPIC ./configure && \
    make && make install && \
    rm -rf /apr-1.5.2.tar.gz /apr-1.5.2

# Compile APR-Util
RUN curl -O -L  http://archive.apache.org/dist/apr/apr-util-1.5.4.tar.gz && \
    tar xvfz apr-util-1.5.4.tar.gz && \
    cd apr-util-1.5.4 && \
    CFLAGS=-fPIC CXXFLAGS=-fPIC ./configure -with-apr=/usr/local/apr && \
    make && make install && \
    rm -rf /apr-util-1.5.4.tar.gz /apr-util-1.5.4

# Libtool
RUN curl -L -O https://ftp.gnu.org/gnu/libtool/libtool-2.4.6.tar.gz && \
    tar xvfz libtool-2.4.6.tar.gz && \
    cd libtool-2.4.6 && \
    ./configure && \
    make && make install && \
    rm -rf /libtool-2.4.6.tar.gz /libtool-2.4.6

# Compile log4cxx
RUN curl -O -L https://github.com/apache/logging-log4cxx/archive/v0.11.0.tar.gz && \
    tar xvfz v0.11.0.tar.gz && \
    cd logging-log4cxx-0.11.0 && \
    ./autogen.sh && \
    CXXFLAGS=-fPIC ./configure && \
    make && make install && \
    rm -rf /v0.11.0.tar.gz /logging-log4cxx-0.11.0

# Compile expat
RUN curl -O -L  https://github.com/libexpat/libexpat/archive/R_2_2_0.tar.gz && \
    tar xfvz R_2_2_0.tar.gz && \
    cd libexpat-R_2_2_0/expat && \
    ./buildconf.sh && \
    CFLAGS=-fPIC CXXFLAGS=-fPIC ./configure  && \
    make && make installlib && \
    rm -rf /R_2_2_0.tar.gz /libexpat-R_2_2_0

RUN curl -O -L https://github.com/Kitware/CMake/archive/v3.12.1.tar.gz && \
    tar xvfz v3.12.1.tar.gz && \
    cd CMake-3.12.1 && \
    ./configure && \
    make && make install && \
    rm -rf /v3.12.1.tar.gz /CMake-3.12.1

# Zstandard
RUN curl -O -L https://github.com/facebook/zstd/releases/download/v1.3.7/zstd-1.3.7.tar.gz && \
    tar xvfz zstd-1.3.7.tar.gz && \
    cd zstd-1.3.7 && \
    CFLAGS="-fPIC -O3" make -j8 && \
    make install && \
    rm -rf /zstd-1.3.7 /zstd-1.3.7.tar.gz

# Snappy
RUN curl -O -L https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz && \
    tar xvfz snappy-1.1.3.tar.gz && \
    cd snappy-1.1.3 && \
    CXXFLAGS="-fPIC -O3" ./configure && \
    make && make install && \
    rm -rf /snappy-1.1.3 /snappy-1.1.3.tar.gz

# LibCurl
RUN curl -O -L  https://github.com/curl/curl/releases/download/curl-7_61_0/curl-7.61.0.tar.gz && \
    tar xvfz curl-7.61.0.tar.gz && \
    cd curl-7.61.0 && \
    CFLAGS=-fPIC ./configure --with-ssl=/usr/local/ssl/ && \
    make && make install && \
    rm -rf /curl-7.61.0.tar.gz /curl-7.61.0

RUN pip install twine
RUN pip install fastavro
RUN pip install six
RUN pip install enum34


ENV PYTHON_INCLUDE_DIR /opt/python/${PYTHON_SPEC}/include
ENV PYTHON_LIBRARIES   /opt/python/${PYTHON_SPEC}/lib/python${PYTHON_VERSION}
ENV OPENSSL_ROOT_DIR   /usr/local/ssl/
