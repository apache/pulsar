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

class Libpulsar < Formula
  desc "Apache Pulsar C++ library"
  homepage "https://pulsar.apache.org"

  head "https://github.com/apache/incubator-pulsar.git"

  # TODO: Switch to official 2.1 version when available
  version "2.1.0-incubating-SNAPSHOT"
  url "https://s3-us-west-2.amazonaws.com/pulsar-preview/apache-pulsar-#{version}-src.tar.gz"
  sha256 "b7ec66c64830f9a0890a145d40a2a28063c056c2d68e9a4e9bdb0cbe7de0bb39"

  option "with-python3", "Use Boost with Python-3.x"
  option "with-log4cxx", "Enable Log4cxx logger"

  depends_on "pkg-config" => :build
  depends_on "cmake" => :build
  depends_on "openssl" => :build
  depends_on "boost" => :build
  depends_on "jsoncpp" => :build
  depends_on "protobuf@2.6" => :build

  if build.with? "python3"
      depends_on "boost-python3" => :build
  else
      depends_on "boost-python" => :build
  end

  if build.with? "log4cxx"
      depends_on "log4cxx" => :build
  end

  def install
    Dir.chdir('pulsar-client-cpp')

    if build.with? "log4cxx"
      system "cmake", ".", "-DBUILD_TESTS=OFF", "-DLINK_STATIC=ON", "-DUSE_LOG4CXX"
    else
      system "cmake", ".", "-DBUILD_TESTS=OFF", "-DLINK_STATIC=ON"
    end
    system "make", "pulsarShared", "pulsarStatic"

    include.install "include/pulsar"
    lib.install "lib/libpulsar.#{version}.dylib"
    lib.install "lib/libpulsar.dylib"
    lib.install "lib/libpulsar.a"
  end
end
