/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <pulsar/defines.h>
#include <pulsar/c/message.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _pulsar_topic_metadata pulsar_topic_metadata_t;

typedef int (*pulsar_message_router)(pulsar_message_t *msg, pulsar_topic_metadata_t *topicMetadata,
                                     void *ctx);

PULSAR_PUBLIC int pulsar_topic_metadata_get_num_partitions(pulsar_topic_metadata_t *topicMetadata);

#ifdef __cplusplus
}
#endif