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

cmake_minimum_required(VERSION 3.0)

find_library(PROTO_LIBRARIES NAMES libprotobuf.a)

find_library(WIRESHARK_LIB wireshark)
find_library(WIRESHARK_UTIL_LIB wsutil)
find_path(WIRESHARK_INCLUDE_PATH wireshark/config.h)

include(FindPkgConfig)
pkg_check_modules(GLIB   glib-2.0)

INCLUDE_DIRECTORIES(${WIRESHARK_INCLUDE_PATH}/wireshark
                    ${GLIB_INCLUDE_DIRS}
                ../lib )

set (CMAKE_CXX_FLAGS "-O3 -g ${CMAKE_CXX_FLAGS}")

ADD_CUSTOM_COMMAND(
         OUTPUT PulsarApi.pb.h PulsarApi.pb.cc
         COMMAND protoc -I ../../pulsar-common/src/main/proto ../../pulsar-common/src/main/proto/PulsarApi.proto --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
         DEPENDS
         ../../pulsar-common/src/main/proto/PulsarApi.proto
         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

ADD_LIBRARY(pulsar SHARED pulsarDissector.cc PulsarApi.pb.h PulsarApi.pb.cc)
SET(CMAKE_SHARED_LIBRARY_PREFIX )
SET(CMAKE_SHARED_LIBRARY_SUFFIX .so)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
endif()

if (APPLE)
    target_link_libraries(pulsar -Wl,-all_load ${PROTO_LIBRARIES})
else ()
    target_link_libraries(pulsar ${PROTOBUF_LIBRARIES})
endif ()
