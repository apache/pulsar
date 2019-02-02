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

#include <pulsar/c/reader.h>
#include <pulsar/c/message.h>
#include <pulsar/c/reader_configuration.h>
#include <pulsar/ReaderConfiguration.h>
#include <pulsar/Reader.h>

#include "c_structs.h"

pulsar_reader_configuration_t *pulsar_reader_configuration_create() {
    return new pulsar_reader_configuration_t;
}

void pulsar_reader_configuration_free(pulsar_reader_configuration_t *configuration) { delete configuration; }

static void message_listener_callback(pulsar::Reader reader, const pulsar::Message &msg,
                                      pulsar_reader_listener listener, void *ctx) {
    pulsar_reader_t c_reader;
    c_reader.reader = reader;
    pulsar_message_t *message = new pulsar_message_t;
    message->message = msg;
    listener(&c_reader, message, ctx);
}

void pulsar_reader_configuration_set_reader_listener(pulsar_reader_configuration_t *configuration,
                                                     pulsar_reader_listener listener, void *ctx) {
    configuration->conf.setReaderListener(
        std::bind(message_listener_callback, std::placeholders::_1, std::placeholders::_2, listener, ctx));
}

int pulsar_reader_configuration_has_reader_listener(pulsar_reader_configuration_t *configuration) {
    return configuration->conf.hasReaderListener();
}

void pulsar_reader_configuration_set_receiver_queue_size(pulsar_reader_configuration_t *configuration,
                                                         int size) {
    configuration->conf.setReceiverQueueSize(size);
}

int pulsar_reader_configuration_get_receiver_queue_size(pulsar_reader_configuration_t *configuration) {
    return configuration->conf.getReceiverQueueSize();
}

void pulsar_reader_configuration_set_reader_name(pulsar_reader_configuration_t *configuration,
                                                 const char *readerName) {
    configuration->conf.setReaderName(readerName);
}

const char *pulsar_reader_configuration_get_reader_name(pulsar_reader_configuration_t *configuration) {
    return configuration->conf.getReaderName().c_str();
}

void pulsar_reader_configuration_set_subscription_role_prefix(pulsar_reader_configuration_t *configuration,
                                                              const char *subscriptionRolePrefix) {
    configuration->conf.setSubscriptionRolePrefix(subscriptionRolePrefix);
}

const char *pulsar_reader_configuration_get_subscription_role_prefix(
    pulsar_reader_configuration_t *configuration) {
    return configuration->conf.getSubscriptionRolePrefix().c_str();
}

void pulsar_reader_configuration_set_read_compacted(pulsar_reader_configuration_t *configuration,
                                                    int readCompacted) {
    configuration->conf.setReadCompacted(readCompacted);
}

int pulsar_reader_configuration_is_read_compacted(pulsar_reader_configuration_t *configuration) {
    return configuration->conf.isReadCompacted();
}