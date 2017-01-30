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

set(SAMPLE_ASYNC_PRODUCER_SOURCES
  SampleAsyncProducer.cc
)

set(SAMPLE_CONSUMER_SOURCES
  SampleConsumer.cc
)

set(SAMPLE_CONSUMER_LISTENER_SOURCES
  SampleConsumerListener.cc
)

set(SAMPLE_PRODUCER_SOURCES
  SampleProducer.cc
)

add_executable(SampleAsyncProducer    ${SAMPLE_ASYNC_PRODUCER_SOURCES})
add_executable(SampleConsumer         ${SAMPLE_CONSUMER_SOURCES})
add_executable(SampleConsumerListener ${SAMPLE_CONSUMER_LISTENER_SOURCES})
add_executable(SampleProducer         ${SAMPLE_PRODUCER_SOURCES})

target_link_libraries(SampleAsyncProducer    ${CLIENT_LIBS})
target_link_libraries(SampleConsumer         ${CLIENT_LIBS})
target_link_libraries(SampleConsumerListener ${CLIENT_LIBS})
target_link_libraries(SampleProducer         ${CLIENT_LIBS})
