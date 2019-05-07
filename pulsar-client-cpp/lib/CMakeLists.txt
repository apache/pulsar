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

file(GLOB PULSAR_SOURCES *.cc *.h lz4/*.c lz4/*.h checksum/*.cc checksum/*.h stats/*.cc stats/*.h c/*.cc c/*.h auth/*.cc auth/*.h auth/athenz/*.cc auth/athenz/*.h)

execute_process(COMMAND python ${CMAKE_SOURCE_DIR}/../src/get-project-version.py OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE PV)
set (CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -D_PULSAR_VERSION_=\\\"${PV}\\\"")

if (NOT PROTOC_PATH)
    set(PROTOC_PATH protoc)
endif()
# Protobuf generation is only supported natively starting from CMake 3.8
# Using custom command for now
ADD_CUSTOM_COMMAND(
         OUTPUT ${CMAKE_SOURCE_DIR}/lib/PulsarApi.pb.h ${CMAKE_SOURCE_DIR}/lib/PulsarApi.pb.cc
         COMMAND ${PROTOC_PATH} -I ../../pulsar-common/src/main/proto ../../pulsar-common/src/main/proto/PulsarApi.proto --cpp_out=${CMAKE_SOURCE_DIR}/lib
         DEPENDS
         ../../pulsar-common/src/main/proto/PulsarApi.proto
         WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(LIBRARY_VERSION $ENV{PULSAR_LIBRARY_VERSION})
if (NOT LIBRARY_VERSION)
    set(LIBRARY_VERSION ${PV})
endif(NOT LIBRARY_VERSION)

if (MSVC)
    find_package(dlfcn-win32 REQUIRED)
    set(CMAKE_DL_LIBS dlfcn-win32::dl psapi.lib)
endif(MSVC)


set(LIB_NAME_SHARED ${LIB_NAME})
if (WIN32)
    string(APPEND LIB_NAME_SHARED dll)
endif()

add_library(pulsarShared SHARED ${PULSAR_SOURCES} PulsarApi.pb.h PulsarApi.pb.cc)
set_target_properties(pulsarShared PROPERTIES OUTPUT_NAME ${LIB_NAME_SHARED} VERSION ${LIBRARY_VERSION})
target_link_libraries(pulsarShared ${COMMON_LIBS} ${CMAKE_DL_LIBS})

add_library(pulsarStatic STATIC ${PULSAR_SOURCES} PulsarApi.pb.h PulsarApi.pb.cc)
set_target_properties(pulsarStatic PROPERTIES OUTPUT_NAME ${LIB_NAME} VERSION ${LIBRARY_VERSION})
target_compile_definitions(pulsarStatic PRIVATE PULSAR_STATIC)

if (MSVC)
    target_include_directories(pulsarStatic PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
    target_include_directories(pulsarShared PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
    target_link_options(pulsarShared PRIVATE $<$<CONFIG:DEBUG>:/NODEFAULTLIB:MSVCRT>)
endif()

# When linking statically, install a libpulsar.a that contains all the
# required dependencies
if (LINK_STATIC)
    if (MSVC)
        
        # This function is to remove either "debug" or "optimized" library names
        # out of the COMMON_LIBS list and return the sanitized list of libraries
        function(remove_libtype LIBLIST LIBTYPE OUTLIST)
            list(FIND LIBLIST ${LIBTYPE} LIST_INDEX)
            while(${LIST_INDEX} GREATER -1)
                list(REMOVE_AT LIBLIST ${LIST_INDEX})
                list(REMOVE_AT LIBLIST ${LIST_INDEX})
                list(FIND LIBLIST ${LIBTYPE} LIST_INDEX)
            endwhile()
            list(REMOVE_ITEM LIBLIST "debug")
            list(REMOVE_ITEM LIBLIST "optimized")
            string(REPLACE ";" " " TEMP_OUT "${LIBLIST}")
            set(${OUTLIST} ${TEMP_OUT} PARENT_SCOPE)
        endfunction(remove_libtype)

        add_library(pulsarStaticWithDeps STATIC ${PULSAR_SOURCES} PulsarApi.pb.h PulsarApi.pb.cc)
        target_include_directories(pulsarStaticWithDeps PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
        remove_libtype("${COMMON_LIBS}" "optimized" DEBUG_STATIC_LIBS)
        remove_libtype("${COMMON_LIBS}" "debug" STATIC_LIBS)
        set_target_properties(pulsarStaticWithDeps PROPERTIES STATIC_LIBRARY_FLAGS_DEBUG ${DEBUG_STATIC_LIBS} STATIC_LIBRARY_FLAGS_RELEASE ${STATIC_LIBS} OUTPUT_NAME ${LIB_NAME}WithDeps VERSION ${LIBRARY_VERSION})
        install(TARGETS pulsarStaticWithDeps DESTINATION lib)
    else()
        # Build a list of the requird .a libs to merge
        SET(STATIC_LIBS "")
        foreach (LIB IN LISTS COMMON_LIBS)
            if (${LIB} MATCHES ".+\\.a$")
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
    endif(MSVC)
else()
    # Install regular libpulsar.a
    target_link_libraries(pulsarStatic ${COMMON_LIBS})
    install(TARGETS pulsarStatic DESTINATION lib)
endif(LINK_STATIC)

install(TARGETS pulsarShared pulsarStatic DESTINATION lib)
install(DIRECTORY "../include/pulsar" DESTINATION include)
