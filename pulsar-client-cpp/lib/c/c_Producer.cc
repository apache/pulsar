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

#include <pulsar/c/producer.h>

#include "c_structs.h"

const char *pulsar_producer_get_topic(pulsar_producer_t *producer) {
    return producer->producer.getTopic().c_str();
}

const char *pulsar_producer_get_producer_name(pulsar_producer_t *producer) {
    return producer->producer.getProducerName().c_str();
}

void pulsar_producer_free(pulsar_producer_t *producer) { delete producer; }

pulsar_result pulsar_producer_send(pulsar_producer_t *producer, pulsar_message_t *msg) {
    msg->message = msg->builder.build();
    return (pulsar_result)producer->producer.send(msg->message);
}

static void handle_producer_send(pulsar::Result result, pulsar_message_t *msg, pulsar_send_callback callback,
                                 void *ctx) {
    callback((pulsar_result)result, msg, ctx);
}

void pulsar_producer_send_async(pulsar_producer_t *producer, pulsar_message_t *msg,
                                pulsar_send_callback callback, void *ctx) {
    msg->message = msg->builder.build();
    producer->producer.sendAsync(msg->message,
                                 std::bind(&handle_producer_send, std::placeholders::_1, msg, callback, ctx));
}

int64_t pulsar_producer_get_last_sequence_id(pulsar_producer_t *producer) {
    return producer->producer.getLastSequenceId();
}

pulsar_result pulsar_producer_close(pulsar_producer_t *producer) {
    return (pulsar_result)producer->producer.close();
}

void pulsar_producer_close_async(pulsar_producer_t *producer, pulsar_close_callback callback, void *ctx) {
    producer->producer.closeAsync(std::bind(handle_result_callback, std::placeholders::_1, callback, ctx));
}

pulsar_result pulsar_producer_flush(pulsar_producer_t *producer) {
    return (pulsar_result)producer->producer.flush();
}

void pulsar_producer_flush_async(pulsar_producer_t *producer, pulsar_close_callback callback, void *ctx) {
    producer->producer.flushAsync(std::bind(handle_result_callback, std::placeholders::_1, callback, ctx));
}
