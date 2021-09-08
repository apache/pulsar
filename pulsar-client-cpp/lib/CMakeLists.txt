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

file(GLOB PULSAR_SOURCES *.cc *.h lz4/*.cc lz4/*.h checksum/*.cc checksum/*.h stats/*.cc stats/*.h c/*.cc c/*.h auth/*.cc auth/*.h auth/athenz/*.cc auth/athenz/*.h)

execute_process(COMMAND python ${CMAKE_SOURCE_DIR}/../src/get-project-version.py OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE PV)
set (CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS} -D_PULSAR_VERSION_=\\\"${PV}\\\"")

if (NOT PROTOC_PATH)
    set(PROTOC_PATH protoc)
endif()

set(LIB_AUTOGEN_DIR ${AUTOGEN_DIR}/lib)
file(MAKE_DIRECTORY ${LIB_AUTOGEN_DIR})
include_directories(${LIB_AUTOGEN_DIR})

# Protobuf generation is only supported natively starting from CMake 3.8
# Using custom command for now
set(PROTO_SOURCES ${LIB_AUTOGEN_DIR}/PulsarApi.pb.cc ${LIB_AUTOGEN_DIR}/PulsarApi.pb.h)
set(PULSAR_SOURCES ${PULSAR_SOURCES} ${PROTO_SOURCES})
ADD_CUSTOM_COMMAND(
         OUTPUT ${PROTO_SOURCES}
         COMMAND ${PROTOC_PATH} -I ../../pulsar-common/src/main/proto ../../pulsar-common/src/main/proto/PulsarApi.proto --cpp_out=${LIB_AUTOGEN_DIR}
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

# this is the "object library" target: compiles the sources only once
add_library(PULSAR_OBJECT_LIB OBJECT ${PULSAR_SOURCES})
set_property(TARGET PULSAR_OBJECT_LIB PROPERTY POSITION_INDEPENDENT_CODE 1)

if (BUILD_DYNAMIC_LIB)
    add_library(pulsarShared SHARED $<TARGET_OBJECTS:PULSAR_OBJECT_LIB>)
    set_property(TARGET pulsarShared PROPERTY OUTPUT_NAME ${LIB_NAME_SHARED})
    set_property(TARGET pulsarShared PROPERTY VERSION ${LIBRARY_VERSION})
    target_link_libraries(pulsarShared ${COMMON_LIBS} ${CMAKE_DL_LIBS})
    if (MSVC)
        target_include_directories(pulsarShared PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
        target_link_options(pulsarShared PRIVATE $<$<CONFIG:DEBUG>:/NODEFAULTLIB:MSVCRT>)
    endif()
endif()


### pulsarSharedNossl not static link ssl, it could avoid rebuild libpulsar when ssl lib need update.
### pulsarSharedNossl is build under condition LINK_STATIC=ON, we should replace static ssl libs with dynamic libs.
SET(COMMON_LIBS_NOSSL ${COMMON_LIBS})
if (NOT ${RECORD_OPENSSL_SSL_LIBRARY} MATCHES ".+\\.a$")
    LIST(REMOVE_ITEM COMMON_LIBS_NOSSL ${OPENSSL_SSL_LIBRARY})
    LIST(APPEND COMMON_LIBS_NOSSL ${RECORD_OPENSSL_SSL_LIBRARY})
endif ()
if (NOT ${RECORD_OPENSSL_CRYPTO_LIBRARY} MATCHES ".+\\.a$")
    LIST(REMOVE_ITEM COMMON_LIBS_NOSSL  ${OPENSSL_CRYPTO_LIBRARY})
    LIST(APPEND COMMON_LIBS_NOSSL ${RECORD_OPENSSL_CRYPTO_LIBRARY})
endif ()

if (BUILD_DYNAMIC_LIB AND LINK_STATIC)
    add_library(pulsarSharedNossl SHARED $<TARGET_OBJECTS:PULSAR_OBJECT_LIB>)
    set_property(TARGET pulsarSharedNossl PROPERTY OUTPUT_NAME ${LIB_NAME_SHARED}nossl)
    set_property(TARGET pulsarSharedNossl PROPERTY VERSION ${LIBRARY_VERSION})
    target_link_libraries(pulsarSharedNossl ${COMMON_LIBS_NOSSL} ${CMAKE_DL_LIBS})
endif()

if (BUILD_STATIC_LIB)
    add_library(pulsarStatic STATIC $<TARGET_OBJECTS:PULSAR_OBJECT_LIB>)
    if (MSVC)
        set_property(TARGET pulsarStatic PROPERTY OUTPUT_NAME "${LIB_NAME}-static")
        target_include_directories(pulsarStatic PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
    else ()
        set_property(TARGET pulsarStatic PROPERTY OUTPUT_NAME ${LIB_NAME})
    endif()
    set_property(TARGET pulsarStatic PROPERTY VERSION ${LIBRARY_VERSION})
    target_compile_definitions(pulsarStatic PRIVATE PULSAR_STATIC)
endif()

# When linking statically, install a libpulsar.a that contains all the
# required dependencies except ssl
if (LINK_STATIC AND BUILD_STATIC_LIB)
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

        add_library(pulsarStaticWithDeps STATIC ${PULSAR_SOURCES})
        target_include_directories(pulsarStaticWithDeps PRIVATE ${dlfcn-win32_INCLUDE_DIRS})
        remove_libtype("${COMMON_LIBS}" "optimized" DEBUG_STATIC_LIBS)
        remove_libtype("${COMMON_LIBS}" "debug" STATIC_LIBS)
        set_property(TARGET pulsarStaticWithDeps PROPERTY STATIC_LIBRARY_FLAGS_DEBUG ${DEBUG_STATIC_LIBS})
        set_property(TARGET pulsarStaticWithDeps PROPERTY STATIC_LIBRARY_FLAGS_RELEASE ${STATIC_LIBS})
        set_property(TARGET pulsarStaticWithDeps PROPERTY OUTPUT_NAME ${LIB_NAME}WithDeps)
        set_property(TARGET pulsarStaticWithDeps PROPERTY VERSION ${LIBRARY_VERSION})
        install(TARGETS pulsarStaticWithDeps DESTINATION lib)
    else()
        # Build a list of the requird .a libs (except ssl) to merge
        SET(STATIC_LIBS "")
        foreach (LIB IN LISTS COMMON_LIBS)
            if (${LIB} MATCHES ".+\\.a$" AND NOT ${LIB} MATCHES ${OPENSSL_SSL_LIBRARY} AND NOT ${LIB} MATCHES ${OPENSSL_CRYPTO_LIBRARY})
                set(STATIC_LIBS "${STATIC_LIBS} ${LIB}")
            endif()
        endforeach()

        add_custom_target(pulsarStaticWithDeps
                ALL
                BYPRODUCTS merged-library
                COMMAND ./build-support/merge_archives.sh libpulsar.a $<TARGET_FILE:pulsarStatic> ${STATIC_LIBS} && mv merged-library/libpulsar.a lib/libpulsarwithdeps.a
                DEPENDS pulsarStatic
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
    endif(MSVC)
elseif(BUILD_STATIC_LIB)
    # Install regular libpulsar.a
    target_link_libraries(pulsarStatic ${COMMON_LIBS})
    install(TARGETS pulsarStatic DESTINATION lib)
endif()

if (BUILD_STATIC_LIB)
    install(TARGETS pulsarStatic DESTINATION lib)
endif()

if (BUILD_DYNAMIC_LIB)
    install(TARGETS pulsarShared RUNTIME DESTINATION bin LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
endif()

if (BUILD_DYNAMIC_LIB AND LINK_STATIC)
    install(TARGETS pulsarSharedNossl RUNTIME DESTINATION bin LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
endif()

install(DIRECTORY "../include/pulsar" DESTINATION include)
