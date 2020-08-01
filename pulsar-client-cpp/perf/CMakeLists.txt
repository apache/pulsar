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

# Test tools
add_definitions(-D_GLIBCXX_USE_NANOSLEEP)

set(PERF_PRODUCER_SOURCES
  PerfProducer.cc
)

set(PERF_CONSUMER_SOURCES
  PerfConsumer.cc
)

add_executable(perfProducer ${PERF_PRODUCER_SOURCES})
add_executable(perfConsumer ${PERF_CONSUMER_SOURCES})

set(TOOL_LIBS ${CLIENT_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_THREAD_LIBRARY})

target_link_libraries(perfProducer pulsarShared ${TOOL_LIBS})
target_link_libraries(perfConsumer pulsarShared ${TOOL_LIBS})
