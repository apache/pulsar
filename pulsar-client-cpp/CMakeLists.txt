#
# Copyright 2016 Yahoo Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cmake_minimum_required(VERSION 2.8)
project (pulsar-cpp)

set(Boost_NO_BOOST_CMAKE ON)

set (CMAKE_CXX_FLAGS "-Wno-deprecated-declarations ${CMAKE_CXX_FLAGS}")

find_package(Boost REQUIRED COMPONENTS program_options filesystem regex thread system)
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ProtoBuf QUIET)
if(NOT ProtoBuf_FOUND)
  find_library(PROTOBUF_LIBRARIES protobuf)
endif()
find_library(LOG4CXX_LIBRARY_PATH log4cxx)
find_library(CURL_LIBRARY_PATH curl)
find_path(LOG4CXX_INCLUDE_PATH log4cxx/logger.h)
find_path(GTEST_INCLUDE_PATH gtest/gtest.h)


include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${Boost_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${ZLIB_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIR}
  ${LOG4CXX_INCLUDE_PATH}
  ${GTEST_INCLUDE_PATH}
)

set(COMMON_LIBS
  ${COMMON_LIBS}
  ${Boost_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${PROTOBUF_LIBRARIES}
  ${LOG4CXX_LIBRARY_PATH}
  ${CURL_LIBRARY_PATH}
)

link_directories(${CMAKE_BINARY_DIR}/lib)

set(CLIENT_LIBS
  ${COMMON_LIBS}
  pulsar
)

add_subdirectory(lib)
add_subdirectory(perf)
add_subdirectory(examples)
add_subdirectory(tests)
