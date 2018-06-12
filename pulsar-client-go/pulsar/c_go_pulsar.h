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

#include <pulsar/c/client.h>
#include <pulsar/c/authentication.h>
#include <stdlib.h>

// Callback proxy functions

void pulsarClientLoggerProxy(pulsar_logger_level_t level, char* file, int line, char* message, void *ctx);

static inline void pulsarClientLoggerConstProxy(pulsar_logger_level_t level, const char* file, int line, const char* message, void *ctx) {
    pulsarClientLoggerProxy(level, (char*)file, line, (char*)message, ctx);
}

static inline void _pulsar_client_configuration_set_logger(pulsar_client_configuration_t *conf, void *ctx) {
    pulsar_client_configuration_set_logger(conf, pulsarClientLoggerConstProxy, ctx);
}

void pulsarCreateProducerCallbackProxy(pulsar_result result, pulsar_producer_t *producer, void *ctx);

static inline void _pulsar_client_create_producer_async(pulsar_client_t *client, const char *topic,
                                                        const pulsar_producer_configuration_t *conf,
                                                        void *ctx) {
    pulsar_client_create_producer_async(client, topic, conf, pulsarCreateProducerCallbackProxy, ctx);
}

void pulsarProducerCloseCallbackProxy(pulsar_result result, void *ctx);

static inline void _pulsar_producer_close_async(pulsar_producer_t *producer, void *ctx) {
    pulsar_producer_close_async(producer, pulsarProducerCloseCallbackProxy, ctx);
}

void pulsarProducerSendCallbackProxy(pulsar_result result, pulsar_message_t *message, void *ctx);

static inline void _pulsar_producer_send_async(pulsar_producer_t *producer, pulsar_message_t *message,
                                               void *ctx) {
    pulsar_producer_send_async(producer, message, pulsarProducerSendCallbackProxy, ctx);
}

int pulsarRouterCallbackProxy(pulsar_message_t *msg, pulsar_topic_metadata_t *topicMetadata, void* ctx);


static inline void _pulsar_producer_configuration_set_message_router(pulsar_producer_configuration_t *conf, void *ctx) {
    pulsar_producer_configuration_set_message_router(conf, pulsarRouterCallbackProxy, ctx);
}

//// Consumer callbacks

void pulsarSubscribeCallbackProxy(pulsar_result result, pulsar_consumer_t *consumer, void *ctx);

static inline void _pulsar_client_subscribe_async(pulsar_client_t *client, const char *topic,
                                                  const char *subscriptionName,
                                                  const pulsar_consumer_configuration_t *conf, void *ctx) {
    pulsar_client_subscribe_async(client, topic, subscriptionName, conf, pulsarSubscribeCallbackProxy, ctx);
}

void pulsarMessageListenerProxy(pulsar_consumer_t *consumer, pulsar_message_t *message, void *ctx);

static inline void _pulsar_consumer_configuration_set_message_listener(
    pulsar_consumer_configuration_t *consumer_configuration, void *ctx) {
    pulsar_consumer_configuration_set_message_listener(consumer_configuration, pulsarMessageListenerProxy,
                                                       ctx);
}

void pulsarConsumerUnsubscribeCallbackProxy(pulsar_result result, void *ctx);

static inline void _pulsar_consumer_unsubscribe_async(pulsar_consumer_t *consumer, void *ctx) {
    pulsar_consumer_unsubscribe_async(consumer, pulsarConsumerUnsubscribeCallbackProxy, ctx);
}

void pulsarConsumerCloseCallbackProxy(pulsar_result result, void *ctx);

static inline void _pulsar_consumer_close_async(pulsar_consumer_t *consumer, void *ctx) {
    pulsar_consumer_close_async(consumer, pulsarConsumerCloseCallbackProxy, ctx);
}

//// Reader callbacks

void pulsarCreateReaderCallbackProxy(pulsar_result result, pulsar_reader_t *reader, void *ctx);

static inline void _pulsar_client_create_reader_async(pulsar_client_t *client, const char *topic,
                                                      const pulsar_message_id_t *startMessageId,
                                                      pulsar_reader_configuration_t *conf, void *ctx) {
    pulsar_client_create_reader_async(client, topic, startMessageId, conf, pulsarCreateReaderCallbackProxy,
                                      ctx);
}

void pulsarReaderListenerProxy(pulsar_reader_t *reader, pulsar_message_t *message, void *ctx);

static inline void _pulsar_reader_configuration_set_reader_listener(
    pulsar_reader_configuration_t *reader_configuration, void *ctx) {
    pulsar_reader_configuration_set_reader_listener(reader_configuration, pulsarReaderListenerProxy, ctx);
}

void pulsarReaderCloseCallbackProxy(pulsar_result result, void *ctx);

static inline void _pulsar_reader_close_async(pulsar_reader_t *reader, void *ctx) {
    pulsar_reader_close_async(reader, pulsarReaderCloseCallbackProxy, ctx);
}


//// String array manipulation

static char** newStringArray(int size) {
    return calloc(sizeof(char*), size);
}

static void setString(char** array, char *str, int n) {
    array[n] = str;
}

static void freeStringArray(char* *array, int size) {
    for (int i = 0; i < size; i++) {
        free(array[i]);
    }

    free(array);
}
