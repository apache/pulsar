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

#include <pulsar/c/result.h>
#include <pulsar/Client.h>

#include <memory>
#include <functional>

struct _pulsar_client {
    std::unique_ptr<pulsar::Client> client;
};

struct _pulsar_client_configuration {
    pulsar::ClientConfiguration conf;
};

struct _pulsar_producer {
    pulsar::Producer producer;
};

struct _pulsar_producer_configuration {
    pulsar::ProducerConfiguration conf;
};

struct _pulsar_consumer {
    pulsar::Consumer consumer;
};

struct _pulsar_consumer_configuration {
    pulsar::ConsumerConfiguration consumerConfiguration;
};

struct _pulsar_reader {
    pulsar::Reader reader;
};

struct _pulsar_reader_configuration {
    pulsar::ReaderConfiguration conf;
};

struct _pulsar_message {
    pulsar::MessageBuilder builder;
    pulsar::Message message;
};

struct _pulsar_message_id {
    pulsar::MessageId messageId;
};

struct _pulsar_authentication {
    pulsar::AuthenticationPtr auth;
};

struct _pulsar_topic_metadata {
    const pulsar::TopicMetadata* metadata;
};

typedef void (*pulsar_result_callback)(pulsar_result res, void* ctx);

static void handle_result_callback(pulsar::Result result, pulsar_result_callback callback, void* ctx) {
    if (callback) {
        callback((pulsar_result)result, ctx);
    }
}

struct _pulsar_string_map {
    std::map<std::string, std::string> map;
};

struct _pulsar_string_list {
    std::vector<std::string> list;
};
