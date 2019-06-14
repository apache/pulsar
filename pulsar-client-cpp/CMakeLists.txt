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

cmake_minimum_required(VERSION 3.4) 
project (pulsar-cpp)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set(CMAKE_CXX_COMPILER_LAUNCHER "ccache")
    MESSAGE(STATUS "Using CCache")
endif(CCACHE_PROGRAM)

option(BUILD_TESTS "Build tests" ON)
MESSAGE(STATUS "BUILD_TESTS:  " ${BUILD_TESTS})

option(BUILD_PYTHON_WRAPPER "Build Pulsar Python wrapper" ON)
MESSAGE(STATUS "BUILD_PYTHON_WRAPPER:  " ${BUILD_PYTHON_WRAPPER})

option(LINK_STATIC "Link against static libraries" OFF)
MESSAGE(STATUS "LINK_STATIC:  " ${LINK_STATIC})

option(USE_LOG4CXX "Build with Log4cxx support" OFF)
MESSAGE(STATUS "USE_LOG4CXX:  " ${USE_LOG4CXX})

IF (CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
ENDIF ()

MESSAGE(STATUS "CMAKE_BUILD_TYPE:  " ${CMAKE_BUILD_TYPE})

set(Boost_NO_BOOST_CMAKE ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_STANDARD 11)

if (NOT MSVC)
    add_compile_options(-msse4.2 -mpclmul -Werror=switch -Wno-deprecated-declarations)
else()
    add_definitions(-DWIN32_LEAN_AND_MEAN -DNOGDI -D_WIN32_WINNT=0x0501 -D_CRT_SECURE_NO_WARNINGS)
    add_compile_options(/wd4244 /wd4267 /wd4018 /wd4715 /wd4251 /wd4275)
endif(NOT MSVC)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(Protobuf_LITE_LIBRARIES $ENV{PROTOBUF_LIBRARIES})
set(LOG_CATEGORY_NAME $ENV{LOG_CATEGORY_NAME})

if (NOT LOG_CATEGORY_NAME)
    set(LOG_CATEGORY_NAME "\"pulsar.\"")
endif(NOT LOG_CATEGORY_NAME)

add_definitions(-DLOG_CATEGORY_NAME=${LOG_CATEGORY_NAME} -DBUILDING_PULSAR -DBOOST_ALL_NO_LIB -DBOOST_ALLOW_DEPRECATED_HEADERS)

if (LINK_STATIC)
    if (USE_LOG4CXX)
        if (LOG4CXX_USE_DYNAMIC_LIBS)
            find_library(LOG4CXX_LIBRARY_PATH log4cxx)
        else ()
            find_library(LOG4CXX_LIBRARY_PATH NAMES liblog4cxx.a)

            # Libraries needed by log4cxx to link statically with
            find_library(APR_LIBRARY_PATH NAMES libapr-1 PATHS /usr/lib /usr/local/apr/lib /usr/local/opt/apr/libexec/lib/)
            find_library(APR_UTIL_LIBRARY_PATH NAMES libaprutil-1 PATHS /usr/lib /usr/local/apr/lib /usr/local/opt/apr-util/libexec/lib/)
            find_library(EXPAT_LIBRARY_PATH NAMES libexpat expat)
            if (APPLE)
                find_library(ICONV_LIBRARY_PATH NAMES libiconv iconv)
            else ()
                set(ICONV_LIBRARY_PATH )
            endif (APPLE)
        endif (LOG4CXX_USE_DYNAMIC_LIBS)
    endif (USE_LOG4CXX)

    if (NOT MSVC)
        set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
    else()
        add_definitions(-DCURL_STATICLIB)
    endif()

    SET(Boost_USE_STATIC_LIBS   ON)
    SET(OPENSSL_USE_STATIC_LIBS TRUE)
else()
    # Link to shared libraries
    if (USE_LOG4CXX)
        find_library(LOG4CXX_LIBRARY_PATH log4cxx)
        find_path(LOG4CXX_INCLUDE_PATH log4cxx/logger.h)
    endif (USE_LOG4CXX)
endif (LINK_STATIC)

find_package(Boost REQUIRED COMPONENTS program_options regex system)

find_package(ZLIB REQUIRED)

find_package(Protobuf QUIET)
if (NOT Protobuf_FOUND OR NOT Protobuf_LITE_LIBRARIES)
    find_library(Protobuf_LITE_LIBRARIES protobuf-lite libprotobuf-lite)
    find_path(Protobuf_INCLUDE_DIRS google/protobuf/stubs/common.h)
endif()

find_package(CURL QUIET)
if (NOT CURL_FOUND)
    find_library(CURL_LIBRARIES NAMES curl libcurl_a)
    find_path(CURL_INCLUDE_DIRS curl/curl.h)
endif()

find_library(LIB_ZSTD zstd libzstd)

find_library(LIB_SNAPPY snappy libsnappy)

if (BUILD_PYTHON_WRAPPER)
    find_package(PythonLibs REQUIRED)
    MESSAGE(STATUS "PYTHON: " ${PYTHONLIBS_VERSION_STRING})

    if (PYTHONLIBS_VERSION_STRING MATCHES "^3.+$")
        MESSAGE(STATUS "DETECTED Python 3")
        string(REPLACE "." ";" PYTHONLIBS_VERSION_NO_LIST ${PYTHONLIBS_VERSION_STRING})
        list(GET PYTHONLIBS_VERSION_NO_LIST 0 PYTHONLIBS_VERSION_MAJOR)
        list(GET PYTHONLIBS_VERSION_NO_LIST 1 PYTHONLIBS_VERSION_MINOR)
        set(BOOST_PYTHON_NAME_POSTFIX ${PYTHONLIBS_VERSION_MAJOR}${PYTHONLIBS_VERSION_MINOR})
        # For python3 the lib name is boost_python3
        set(BOOST_PYTHON_NAME_LIST python3;python3-mt;python-py${BOOST_PYTHON_NAME_POSTFIX};python${BOOST_PYTHON_NAME_POSTFIX}-mt;python${BOOST_PYTHON_NAME_POSTFIX})
    else ()
        # Regular boost_python
        set(BOOST_PYTHON_NAME_LIST python;python-mt;python-py27;python27-mt;python27)
    endif ()

    foreach (BOOST_PYTHON_NAME IN LISTS BOOST_PYTHON_NAME_LIST)
        find_package(Boost QUIET COMPONENTS ${BOOST_PYTHON_NAME})
        if (${Boost_FOUND})
            set(BOOST_PYTHON_NAME_FOUND ${BOOST_PYTHON_NAME})
            break()
        endif()
    endforeach()

    if (NOT ${Boost_FOUND})
        MESSAGE(FATAL_ERROR "Could not find Boost Python library")
    endif ()

    find_package(Boost REQUIRED COMPONENTS ${BOOST_PYTHON_NAME_FOUND})
endif (BUILD_PYTHON_WRAPPER)

if (APPLE)
    set(OPENSSL_INCLUDE_DIR /usr/local/opt/openssl/include/)
    set(OPENSSL_ROOT_DIR /usr/local/opt/openssl/)
endif ()

find_package(OpenSSL REQUIRED)

if (BUILD_TESTS)
    find_path(GTEST_INCLUDE_PATH gtest/gtest.h)
    find_path(GMOCK_INCLUDE_PATH gmock/gmock.h)
endif ()

if (USE_LOG4CXX)
    set(CMAKE_CXX_FLAGS " -DUSE_LOG4CXX ${CMAKE_CXX_FLAGS}")
    find_path(LOG4CXX_INCLUDE_PATH log4cxx/logger.h)
endif (USE_LOG4CXX)

if (NOT APPLE AND NOT MSVC)
    # we don't set options below to build _pulsar.so
    set(CMAKE_CXX_FLAGS_PYTHON "${CMAKE_CXX_FLAGS}")
    # Hide all non-exported symbols to avoid conflicts
    add_compile_options(-fvisibility=hidden -Wl,--exclude-libs,ALL)
endif ()

if (LIB_ZSTD)
    set(HAS_ZSTD 1)
else ()
    set(HAS_ZSTD 0)
endif ()
MESSAGE(STATUS "HAS_ZSTD: ${HAS_ZSTD}")

if (LIB_SNAPPY)
    set(HAS_SNAPPY 1)
else ()
    set(HAS_SNAPPY 0)
endif ()
MESSAGE(STATUS "HAS_SNAPPY: ${HAS_SNAPPY}")

set(ADDITIONAL_LIBRARIES $ENV{PULSAR_ADDITIONAL_LIBRARIES})
link_directories( $ENV{PULSAR_ADDITIONAL_LIBRARY_PATH} )

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/include
  ${Boost_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${ZLIB_INCLUDE_DIRS}
  ${CURL_INCLUDE_DIRS}
  ${Protobuf_INCLUDE_DIRS}
  ${LOG4CXX_INCLUDE_PATH}
  ${GTEST_INCLUDE_PATH}
  ${GMOCK_INCLUDE_PATH}
)

set(COMMON_LIBS
  ${COMMON_LIBS}
  ${Boost_REGEX_LIBRARY}
  ${Boost_SYSTEM_LIBRARY}
  ${CURL_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${Protobuf_LITE_LIBRARIES}
  ${ADDITIONAL_LIBRARIES}
)

if (NOT MSVC)
    set(COMMON_LIBS
    ${COMMON_LIBS} -lpthread -lm
    dl
    pthread
    )
else()
    set(COMMON_LIBS
    ${COMMON_LIBS} 
    wldap32.lib
    Normaliz.lib)
endif()

if (USE_LOG4CXX)
    set(COMMON_LIBS
      ${COMMON_LIBS}
      ${LOG4CXX_LIBRARY_PATH}
      ${APR_LIBRARY_PATH}
      ${APR_UTIL_LIBRARY_PATH}
      ${EXPAT_LIBRARY_PATH}
      ${ICONV_LIBRARY_PATH}
    )
endif ()

if (HAS_ZSTD)
    set(COMMON_LIBS ${COMMON_LIBS} ${LIB_ZSTD} )
endif ()

add_definitions(-DHAS_ZSTD=${HAS_ZSTD})

if (HAS_SNAPPY)
    set(COMMON_LIBS ${COMMON_LIBS} ${LIB_SNAPPY} )
endif ()

add_definitions(-DHAS_SNAPPY=${HAS_SNAPPY})

if(NOT APPLE AND NOT MSVC)
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

if (BUILD_TESTS)
    add_subdirectory(tests)
endif()

if (BUILD_PYTHON_WRAPPER)
    add_subdirectory(python)
endif ()

# `make format` option
if (NOT APPLE AND NOT WIN32)
    set(CLANG_FORMAT_VERSION "5.0")
endif()


find_package(ClangTools)
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")
add_custom_target(format python ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        0
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        ${CMAKE_SOURCE_DIR}/lib
        ${CMAKE_SOURCE_DIR}/tests
        ${CMAKE_SOURCE_DIR}/include)

# `make check-format` option (for CI test)
add_custom_target(check-format python ${BUILD_SUPPORT_DIR}/run_clang_format.py
        ${CLANG_FORMAT_BIN}
        1
        ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
        ${CMAKE_SOURCE_DIR}/lib
        ${CMAKE_SOURCE_DIR}/tests
        ${CMAKE_SOURCE_DIR}/include)
