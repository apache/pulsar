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

#ifdef __cplusplus
extern "C" {
#endif

typedef struct _pulsar_reader_configuration pulsar_reader_configuration_t;

typedef void (*pulsar_reader_listener)(pulsar_reader_t *reader, pulsar_message_t *msg, void *ctx);

PULSAR_PUBLIC pulsar_reader_configuration_t *pulsar_reader_configuration_create();

PULSAR_PUBLIC void pulsar_reader_configuration_free(pulsar_reader_configuration_t *configuration);

/**
 * A message listener enables your application to configure how to process
 * messages. A listener will be called in order for every message received.
 */
PULSAR_PUBLIC void pulsar_reader_configuration_set_reader_listener(
    pulsar_reader_configuration_t *configuration, pulsar_reader_listener listener, void *ctx);

PULSAR_PUBLIC int pulsar_reader_configuration_has_reader_listener(
    pulsar_reader_configuration_t *configuration);

/**
 * Sets the size of the reader receive queue.
 *
 * The consumer receive queue controls how many messages can be accumulated by the Consumer before the
 * application calls receive(). Using a higher value could potentially increase the consumer throughput
 * at the expense of bigger memory utilization.
 *
 * Setting the consumer queue size as zero decreases the throughput of the consumer, by disabling
 * pre-fetching of
 * messages. This approach improves the message distribution on shared subscription, by pushing messages
 * only to
 * the consumers that are ready to process them. Neither receive with timeout nor Partitioned Topics can
 * be
 * used if the consumer queue size is zero. The receive() function call should not be interrupted when
 * the consumer queue size is zero.
 *
 * Default value is 1000 messages and should be good for most use cases.
 *
 * @param size
 *            the new receiver queue size value
 */
PULSAR_PUBLIC void pulsar_reader_configuration_set_receiver_queue_size(
    pulsar_reader_configuration_t *configuration, int size);

PULSAR_PUBLIC int pulsar_reader_configuration_get_receiver_queue_size(
    pulsar_reader_configuration_t *configuration);

PULSAR_PUBLIC void pulsar_reader_configuration_set_reader_name(pulsar_reader_configuration_t *configuration,
                                                               const char *readerName);

PULSAR_PUBLIC const char *pulsar_reader_configuration_get_reader_name(
    pulsar_reader_configuration_t *configuration);

PULSAR_PUBLIC void pulsar_reader_configuration_set_subscription_role_prefix(
    pulsar_reader_configuration_t *configuration, const char *subscriptionRolePrefix);

PULSAR_PUBLIC const char *pulsar_reader_configuration_get_subscription_role_prefix(
    pulsar_reader_configuration_t *configuration);

PULSAR_PUBLIC void pulsar_reader_configuration_set_read_compacted(
    pulsar_reader_configuration_t *configuration, int readCompacted);

PULSAR_PUBLIC int pulsar_reader_configuration_is_read_compacted(pulsar_reader_configuration_t *configuration);

#ifdef __cplusplus
}
#endif
