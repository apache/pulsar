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

find_library(GMOCK_LIBRARY_PATH gmock)
find_library(GTEST_LIBRARY_PATH gtest)
find_library(GMOCKD_LIBRARY_PATH gmockd)
find_library(GTESTD_LIBRARY_PATH gtestd)
if (NOT GMOCKD_LIBRARY_PATH)
    set(GMOCKD_LIBRARY_PATH ${GMOCK_LIBRARY_PATH})
endif()
if (NOT GTESTD_LIBRARY_PATH)
    set(GTESTD_LIBRARY_PATH ${GTEST_LIBRARY_PATH})
endif()

file(GLOB TEST_SOURCES *.cc)

add_executable(main ${TEST_SOURCES})

target_include_directories(main PRIVATE ${CMAKE_SOURCE_DIR}/lib)

target_link_libraries(main ${CLIENT_LIBS} pulsarShared $<$<CONFIG:Debug>:${GMOCKD_LIBRARY_PATH}> $<$<CONFIG:Debug>:${GTESTD_LIBRARY_PATH}> $<$<NOT:$<CONFIG:Debug>>:${GMOCK_LIBRARY_PATH}> $<$<NOT:$<CONFIG:Debug>>:${GTEST_LIBRARY_PATH}>)
