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

file(GLOB PULSAR_SOURCES *.cc lz4/*.c checksum/*.cc stats/*.cc c/*.cc auth/*.cc auth/athenz/*.cc)

execute_process(COMMAND ../src/get-project-version.py OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE PV)
set (CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -D_PULSAR_VERSION_=\\\"${PV}\\\"")

# Protobuf generation is only supported natively starting from CMake 3.8
# Using custom command for now
ADD_CUSTOM_COMMAND(
         OUTPUT PulsarApi.pb.h PulsarApi.pb.cc
         COMMAND protoc -I ../../pulsar-common/src/main/proto ../../pulsar-common/src/main/proto/PulsarApi.proto --cpp_out=${CMAKE_SOURCE_DIR}/lib
         DEPENDS
         ../../pulsar-common/src/main/proto/PulsarApi.proto
         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(LIBRARY_VERSION $ENV{PULSAR_LIBRARY_VERSION})
if (NOT LIBRARY_VERSION)
    set(LIBRARY_VERSION ${PV})
endif(NOT LIBRARY_VERSION)


add_library(pulsarShared SHARED ${PULSAR_SOURCES} PulsarApi.pb.h PulsarApi.pb.cc)
set_target_properties(pulsarShared PROPERTIES OUTPUT_NAME ${LIB_NAME} VERSION ${LIBRARY_VERSION})
target_link_libraries(pulsarShared ${COMMON_LIBS})

add_library(pulsarStatic STATIC ${PULSAR_SOURCES} PulsarApi.pb.h PulsarApi.pb.cc)
set_target_properties(pulsarStatic PROPERTIES OUTPUT_NAME ${LIB_NAME} VERSION ${LIBRARY_VERSION})

# When linking statically, install a libpulsar.a that contains all the
# required dependencies
if (LINK_STATIC)
    # Build a list of the requird .a libs to merge
    SET(STATIC_LIBS "")
    foreach (LIB IN LISTS COMMON_LIBS)
        if (${LIB} MATCHES ".+\\.a")
            set(STATIC_LIBS "${STATIC_LIBS} ${LIB}")
        endif()
    endforeach()

    add_custom_target(pulsarStaticWithDeps
            ALL
            BYPRODUCTS merged-library
            COMMAND ./build-support/merge_archives.sh libpulsar.a $<TARGET_FILE:pulsarStatic> ${STATIC_LIBS}
            DEPENDS pulsarStatic
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
    install(FILES ../merged-library/libpulsar.a DESTINATION lib)
else()
    # Install regular libpulsar.a
    target_link_libraries(pulsarStatic ${COMMON_LIBS})
    install(TARGETS pulsarStatic DESTINATION lib)
endif(LINK_STATIC)

install(TARGETS pulsarShared DESTINATION lib)
install(DIRECTORY "../include/pulsar" DESTINATION include)
