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

INCLUDE_DIRECTORIES("${Boost_INCLUDE_DIRS}" "${PYTHON_INCLUDE_DIRS}")

ADD_LIBRARY(_pulsar SHARED src/pulsar.cc src/producer.cc src/consumer.cc
                            src/config.cc src/enums.cc src/client.cc
                            src/message.cc src/authentication.cc)
SET(CMAKE_SHARED_LIBRARY_PREFIX )
SET(CMAKE_SHARED_LIBRARY_SUFFIX .so)

set (CMAKE_SHARED_LINKER_FLAGS " -static -undefined dynamic_lookup ")

if (APPLE)
    set_target_properties(_pulsar PROPERTIES
                LINK_FLAGS "-Wl,-all_load")
else ()
    set_target_properties(_pulsar PROPERTIES
        LINK_FLAGS "-Wl,--whole-archive")
endif ()

TARGET_LINK_LIBRARIES(_pulsar ${Boost_LIBRARIES} ${CLIENT_LIBS})
