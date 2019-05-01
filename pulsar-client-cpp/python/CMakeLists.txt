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

ADD_LIBRARY(_pulsar SHARED src/pulsar.cc src/producer.cc src/consumer.cc
                            src/config.cc src/enums.cc src/client.cc
                            src/message.cc src/authentication.cc
                            src/reader.cc src/schema.cc)
SET(CMAKE_SHARED_LIBRARY_PREFIX )
SET(CMAKE_SHARED_LIBRARY_SUFFIX .so)


if (CMAKE_CXX_FLAGS_PYTHON)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_PYTHON}")
endif()

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
endif()

# Newer boost versions don't use the -mt suffix
if (NOT DEFINED ${Boost_PYTHON27-MT_LIBRARY})
  set(Boost_PYTHON27-MT_LIBRARY ${Boost_PYTHON27_LIBRARY})
endif()

if (NOT DEFINED ${Boost_PYTHON37-MT_LIBRARY})
  set(Boost_PYTHON37-MT_LIBRARY ${Boost_PYTHON37_LIBRARY})
endif()

# Try all possible boost-python variable namings
set(PYTHON_WRAPPER_LIBS ${Boost_PYTHON_LIBRARY} ${Boost_PYTHON3_LIBRARY}
                        ${Boost_PYTHON27-MT_LIBRARY} ${Boost_PYTHON37-MT_LIBRARY}
                        ${Boost_PYTHON34_LIBRARY} ${Boost_PYTHON35_LIBRARY}
                        ${Boost_PYTHON36_LIBRARY})

if (APPLE)
    set(PYTHON_WRAPPER_LIBS ${PYTHON_LIBRARIES} ${PYTHON_WRAPPER_LIBS}
                         ${Boost_PYTHON27-MT_LIBRARY_RELEASE} ${Boost_PYTHON37-MT_LIBRARY_RELEASE})

    if (LINK_STATIC)
        # When linking statically on MacOS, include also libicu since it's now required by boost::regex
        find_library(ICU_DATA REQUIRED NAMES libicudata.a PATHS /usr/local/opt/icu4c/lib)
        find_library(ICU_I18N REQUIRED NAMES libicui18n.a PATHS /usr/local/opt/icu4c/lib)
        find_library(ICU_UUC REQUIRED NAMES libicuuc.a PATHS /usr/local/opt/icu4c/lib)

        set(ICU_LIBS ${ICU_DATA} ${ICU_I18N} ${ICU_UUC})
    endif ()
endif()

message(STATUS "Using Boost Python libs: ${PYTHON_WRAPPER_LIBS}")

if (NOT PYTHON_WRAPPER_LIBS)
    MESSAGE(FATAL_ERROR "Could not find Boost Python library")
endif ()

if (APPLE)
    target_link_libraries(_pulsar -Wl,-all_load pulsarStatic ${PYTHON_WRAPPER_LIBS} ${COMMON_LIBS} ${ICU_LIBS})
else ()
    if (NOT MSVC)
      set (CMAKE_SHARED_LINKER_FLAGS " -static-libgcc  -static-libstdc++")
    endif()
    target_link_libraries(_pulsar pulsarStatic ${PYTHON_WRAPPER_LIBS} ${COMMON_LIBS})
endif ()
