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

INCLUDE_DIRECTORIES("${Boost_INCLUDE_DIRS}" "${PYTHON_INCLUDE_DIRS}")

ADD_LIBRARY(_pulsar SHARED src/pulsar.cc
                           src/producer.cc
                           src/consumer.cc
                           src/config.cc
                           src/enums.cc
                           src/client.cc
                           src/message.cc
                           src/authentication.cc
                           src/reader.cc
                           src/schema.cc
                           src/cryptoKeyReader.cc
                           src/exceptions.cc)

SET(CMAKE_SHARED_LIBRARY_PREFIX )
SET(CMAKE_SHARED_LIBRARY_SUFFIX .so)

if (NOT APPLE AND NOT MSVC)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_PYTHON}")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -Qunused-arguments -undefined dynamic_lookup")
endif()

# Newer boost versions don't use the -mt suffix
if (NOT DEFINED ${Boost_PYTHON27-MT_LIBRARY})
  set(Boost_PYTHON27-MT_LIBRARY ${Boost_PYTHON27_LIBRARY})
endif()

if (NOT DEFINED ${Boost_PYTHON37-MT_LIBRARY})
  set(Boost_PYTHON37-MT_LIBRARY ${Boost_PYTHON37_LIBRARY})
endif()

if (NOT DEFINED ${Boost_PYTHON38-MT_LIBRARY})
  set(Boost_PYTHON38-MT_LIBRARY ${Boost_PYTHON38_LIBRARY})
endif()

if (NOT DEFINED ${Boost_PYTHON39-MT_LIBRARY})
  set(Boost_PYTHON39-MT_LIBRARY ${Boost_PYTHON39_LIBRARY})
endif()

# Try all possible boost-python variable namings
set(PYTHON_WRAPPER_LIBS ${Boost_PYTHON_LIBRARY}
                        ${Boost_PYTHON3_LIBRARY}
                        ${Boost_PYTHON27-MT_LIBRARY}
                        ${Boost_PYTHON37-MT_LIBRARY}
                        ${Boost_PYTHON34_LIBRARY}
                        ${Boost_PYTHON35_LIBRARY}
                        ${Boost_PYTHON36_LIBRARY}
                        ${Boost_PYTHON38_LIBRARY}
                        ${Boost_PYTHON39_LIBRARY})

if (APPLE)
    if (Boost_PYTHON27-MT_LIBRARY_RELEASE)
        set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} ${Boost_PYTHON27-MT_LIBRARY_RELEASE})
    endif ()
    if (Boost_PYTHON37-MT_LIBRARY_RELEASE)
        set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} ${Boost_PYTHON37-MT_LIBRARY_RELEASE})
    endif ()
    if (Boost_PYTHON38-MT_LIBRARY_RELEASE)
        set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} ${Boost_PYTHON38-MT_LIBRARY_RELEASE})
    endif ()
    if (Boost_PYTHON39-MT_LIBRARY_RELEASE)
        set(PYTHON_WRAPPER_LIBS ${PYTHON_WRAPPER_LIBS} ${Boost_PYTHON39-MT_LIBRARY_RELEASE})
    endif ()
endif()

message(STATUS "Using Boost Python libs: ${PYTHON_WRAPPER_LIBS}")

if (NOT PYTHON_WRAPPER_LIBS)
    MESSAGE(FATAL_ERROR "Could not find Boost Python library")
endif ()

if (APPLE)
    set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
    target_link_libraries(_pulsar -Wl,-all_load pulsarStatic ${PYTHON_WRAPPER_LIBS} ${COMMON_LIBS} ${ICU_LIBS})
else ()
    if (NOT MSVC)
      set (CMAKE_SHARED_LINKER_FLAGS " -static-libgcc  -static-libstdc++")
    endif()
    target_link_libraries(_pulsar pulsarStatic ${PYTHON_WRAPPER_LIBS} ${COMMON_LIBS})
endif ()
