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

cmake_minimum_required(VERSION 2.8)
project (pulsar-cpp)

option(LINK_STATIC "Link against static libraries" OFF)
MESSAGE(STATUS "LINK_STATIC:  " ${LINK_STATIC})

set(Boost_NO_BOOST_CMAKE ON)

if (NOT CXX_STANDARD)
    set(CXX_STANDARD "-std=c++11")
endif(NOT CXX_STANDARD)

set(CMAKE_CXX_FLAGS "-O3 -g -Wno-deprecated-declarations ${CXX_STANDARD} ${CMAKE_CXX_FLAGS}")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(PROTOBUF_LIBRARIES $ENV{PROTOBUF_LIBRARIES})
set(LOG_CATEGORY_NAME $ENV{LOG_CATEGORY_NAME})

if (NOT LOG_CATEGORY_NAME)
    set(LOG_CATEGORY_NAME "\"pulsar.\"")
endif(NOT LOG_CATEGORY_NAME)

add_definitions(-DLOG_CATEGORY_NAME=${LOG_CATEGORY_NAME})

if (LINK_STATIC)
    SET(Boost_USE_STATIC_LIBS   ON)
    SET(OPENSSL_USE_STATIC_LIBS TRUE)

    find_library(ZLIB_LIBRARY_PATH REQUIRED NAMES libz.a z)
    find_library(PROTOBUF_LIBRARIES NAMES libprotobuf.a)
    find_library(CURL_LIBRARY_PATH NAMES libcurl.a curl)
    find_library(LIB_JSON NAMES libjsoncpp.a)
    find_library(LOG4CXX_LIBRARY_PATH NAMES liblog4cxx.a)

    # Libraries needed by log4cxx to link statically with
    find_library(APR_LIBRARY_PATH NAMES libapr-1.a PATHS /usr/lib /usr/local/apr/lib /usr/local/opt/apr/libexec/lib/)
    find_library(APR_UTIL_LIBRARY_PATH NAMES libaprutil-1.a PATHS /usr/lib /usr/local/apr/lib /usr/local/opt/apr-util/libexec/lib/)
    find_library(EXPAT_LIBRARY_PATH NAMES libexpat.a expat)
    if (APPLE)
        find_library(ICONV_LIBRARY_PATH NAMES libiconv.a iconv)
    else ()
        set(ICONV_LIBRARY_PATH )
    endif ()
else()
    # Link to shared libraries
    find_package(ZLIB REQUIRED)
    if (NOT PROTOBUF_LIBRARIES)
      find_package(ProtoBuf QUIET)
      if (NOT ProtoBuf_FOUND)
        find_library(PROTOBUF_LIBRARIES protobuf)
      endif (NOT ProtoBuf_FOUND)
    endif (NOT PROTOBUF_LIBRARIES)

    find_library(LIB_JSON jsoncpp)
    find_library(LOG4CXX_LIBRARY_PATH log4cxx)
    find_library(CURL_LIBRARY_PATH curl)
    find_path(LOG4CXX_INCLUDE_PATH log4cxx/logger.h)
endif (LINK_STATIC)

find_package(Boost REQUIRED COMPONENTS program_options filesystem regex
                                        thread system python)
find_package(PythonLibs REQUIRED)


find_package(OpenSSL REQUIRED)
find_path(GTEST_INCLUDE_PATH gtest/gtest.h)
find_path(JSON_INCLUDE_PATH jsoncpp)
find_path(LOG4CXX_INCLUDE_PATH log4cxx/logger.h)

if (NOT LIB_JSON)
    find_library(LIB_JSON json_cpp)
endif (NOT LIB_JSON)

if (NOT JSON_INCLUDE_PATH)
    find_path(JSON_INCLUDE_PATH json)
else (NOT JSON_INCLUDE_PATH)
    set(JSON_INCLUDE_PATH ${JSON_INCLUDE_PATH}/jsoncpp/)
endif (NOT JSON_INCLUDE_PATH)

set(ADDITIONAL_LIBRARIES $ENV{PULSAR_ADDITIONAL_LIBRARIES})
link_directories( $ENV{PULSAR_ADDITIONAL_LIBRARY_PATH} )

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${Boost_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${ZLIB_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIR}
  ${LOG4CXX_INCLUDE_PATH}
  ${GTEST_INCLUDE_PATH}
  ${JSON_INCLUDE_PATH}
)

set(COMMON_LIBS
  ${COMMON_LIBS} -lpthread -lm
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${Boost_FILESYSTEM_LIBRARY}
  ${Boost_REGEX_LIBRARY}
  ${Boost_THREAD_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${Boost_CHRONO_LIBRARY}
  ${Boost_DATE_TIME_LIBRARY}
  ${Boost_ATOMIC_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARY_PATH}
  ${PROTOBUF_LIBRARIES}
  ${LOG4CXX_LIBRARY_PATH}
  ${APR_LIBRARY_PATH}
  ${APR_UTIL_LIBRARY_PATH}
  ${EXPAT_LIBRARY_PATH}
  ${ICONV_LIBRARY_PATH}
  ${CURL_LIBRARY_PATH}
  ${ADDITIONAL_LIBRARIES}
  ${LIB_JSON}
  dl
  pthread
)

if(NOT APPLE)
    set(COMMON_LIBS ${COMMON_LIBS} rt)
endif ()

link_directories(${CMAKE_BINARY_DIR}/lib)

set(LIB_NAME $ENV{PULSAR_LIBRARY_NAME})
if (NOT LIB_NAME)
    set(LIB_NAME pulsar)
endif(NOT LIB_NAME)

set(CLIENT_LIBS
  ${COMMON_LIBS}
  ${LIB_NAME}
)

add_subdirectory(lib)
add_subdirectory(perf)
add_subdirectory(examples)
add_subdirectory(tests)
add_subdirectory(python)
