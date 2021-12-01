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

#include <pulsar/c/client.h>

#include "c_structs.h"

pulsar_client_t *pulsar_client_create(const char *serviceUrl,
                                      const pulsar_client_configuration_t *clientConfiguration) {
    pulsar_client_t *c_client = new pulsar_client_t;
    c_client->client.reset(new pulsar::Client(std::string(serviceUrl), clientConfiguration->conf));
    return c_client;
}

void pulsar_client_free(pulsar_client_t *client) { delete client; }

pulsar_result pulsar_client_create_producer(pulsar_client_t *client, const char *topic,
                                            const pulsar_producer_configuration_t *conf,
                                            pulsar_producer_t **c_producer) {
    pulsar::Producer producer;
    pulsar::Result res = client->client->createProducer(topic, conf->conf, producer);
    if (res == pulsar::ResultOk) {
        (*c_producer) = new pulsar_producer_t;
        (*c_producer)->producer = producer;
        return pulsar_result_Ok;
    } else {
        return (pulsar_result)res;
    }
}

static void handle_create_producer_callback(pulsar::Result result, pulsar::Producer producer,
                                            pulsar_create_producer_callback callback, void *ctx) {
    if (result == pulsar::ResultOk) {
        pulsar_producer_t *c_producer = new pulsar_producer_t;
        c_producer->producer = producer;
        callback(pulsar_result_Ok, c_producer, ctx);
    } else {
        callback((pulsar_result)result, NULL, ctx);
    }
}

void pulsar_client_create_producer_async(pulsar_client_t *client, const char *topic,
                                         const pulsar_producer_configuration_t *conf,
                                         pulsar_create_producer_callback callback, void *ctx) {
    client->client->createProducerAsync(topic, conf->conf,
                                        std::bind(&handle_create_producer_callback, std::placeholders::_1,
                                                  std::placeholders::_2, callback, ctx));
}

pulsar_result pulsar_client_subscribe(pulsar_client_t *client, const char *topic,
                                      const char *subscriptionName,
                                      const pulsar_consumer_configuration_t *conf,
                                      pulsar_consumer_t **c_consumer) {
    pulsar::Consumer consumer;
    pulsar::Result res =
        client->client->subscribe(topic, subscriptionName, conf->consumerConfiguration, consumer);
    if (res == pulsar::ResultOk) {
        (*c_consumer) = new pulsar_consumer_t;
        (*c_consumer)->consumer = consumer;
        return pulsar_result_Ok;
    } else {
        return (pulsar_result)res;
    }
}

static void handle_subscribe_callback(pulsar::Result result, pulsar::Consumer consumer,
                                      pulsar_subscribe_callback callback, void *ctx) {
    if (result == pulsar::ResultOk) {
        pulsar_consumer_t *c_consumer = new pulsar_consumer_t;
        c_consumer->consumer = consumer;
        callback(pulsar_result_Ok, c_consumer, ctx);
    } else {
        callback((pulsar_result)result, NULL, ctx);
    }
}

void pulsar_client_subscribe_async(pulsar_client_t *client, const char *topic, const char *subscriptionName,
                                   const pulsar_consumer_configuration_t *conf,
                                   pulsar_subscribe_callback callback, void *ctx) {
    client->client->subscribeAsync(
        topic, subscriptionName, conf->consumerConfiguration,
        std::bind(&handle_subscribe_callback, std::placeholders::_1, std::placeholders::_2, callback, ctx));
}

pulsar_result pulsar_client_subscribe_multi_topics(pulsar_client_t *client, const char **topics,
                                                   int topicsCount, const char *subscriptionName,
                                                   const pulsar_consumer_configuration_t *conf,
                                                   pulsar_consumer_t **c_consumer) {
    pulsar::Consumer consumer;
    std::vector<std::string> topicsList;
    for (int i = 0; i < topicsCount; i++) {
        topicsList.push_back(topics[i]);
    }

    pulsar::Result res =
        client->client->subscribe(topicsList, subscriptionName, conf->consumerConfiguration, consumer);
    if (res == pulsar::ResultOk) {
        (*c_consumer) = new pulsar_consumer_t;
        (*c_consumer)->consumer = consumer;
        return pulsar_result_Ok;
    } else {
        return (pulsar_result)res;
    }
}

void pulsar_client_subscribe_multi_topics_async(pulsar_client_t *client, const char **topics, int topicsCount,
                                                const char *subscriptionName,
                                                const pulsar_consumer_configuration_t *conf,
                                                pulsar_subscribe_callback callback, void *ctx) {
    std::vector<std::string> topicsList;
    for (int i = 0; i < topicsCount; i++) {
        topicsList.push_back(topics[i]);
    }

    client->client->subscribeAsync(
        topicsList, subscriptionName, conf->consumerConfiguration,
        std::bind(&handle_subscribe_callback, std::placeholders::_1, std::placeholders::_2, callback, ctx));
}

pulsar_result pulsar_client_subscribe_pattern(pulsar_client_t *client, const char *topicPattern,
                                              const char *subscriptionName,
                                              const pulsar_consumer_configuration_t *conf,
                                              pulsar_consumer_t **c_consumer) {
    pulsar::Consumer consumer;
    pulsar::Result res = client->client->subscribeWithRegex(topicPattern, subscriptionName,
                                                            conf->consumerConfiguration, consumer);
    if (res == pulsar::ResultOk) {
        (*c_consumer) = new pulsar_consumer_t;
        (*c_consumer)->consumer = consumer;
        return pulsar_result_Ok;
    } else {
        return (pulsar_result)res;
    }
}

void pulsar_client_subscribe_pattern_async(pulsar_client_t *client, const char *topicPattern,
                                           const char *subscriptionName,
                                           const pulsar_consumer_configuration_t *conf,
                                           pulsar_subscribe_callback callback, void *ctx) {
    client->client->subscribeWithRegexAsync(
        topicPattern, subscriptionName, conf->consumerConfiguration,
        std::bind(&handle_subscribe_callback, std::placeholders::_1, std::placeholders::_2, callback, ctx));
}

pulsar_result pulsar_client_create_reader(pulsar_client_t *client, const char *topic,
                                          const pulsar_message_id_t *startMessageId,
                                          pulsar_reader_configuration_t *conf, pulsar_reader_t **c_reader) {
    pulsar::Reader reader;
    pulsar::Result res = client->client->createReader(topic, startMessageId->messageId, conf->conf, reader);
    if (res == pulsar::ResultOk) {
        (*c_reader) = new pulsar_reader_t;
        (*c_reader)->reader = reader;
        return pulsar_result_Ok;
    } else {
        return (pulsar_result)res;
    }
}

static void handle_reader_callback(pulsar::Result result, pulsar::Reader reader,
                                   pulsar_reader_callback callback, void *ctx) {
    if (result == pulsar::ResultOk) {
        pulsar_reader_t *c_reader = new pulsar_reader_t;
        c_reader->reader = reader;
        callback(pulsar_result_Ok, c_reader, ctx);
    } else {
        callback((pulsar_result)result, NULL, ctx);
    }
}

void pulsar_client_create_reader_async(pulsar_client_t *client, const char *topic,
                                       const pulsar_message_id_t *startMessageId,
                                       pulsar_reader_configuration_t *conf, pulsar_reader_callback callback,
                                       void *ctx) {
    client->client->createReaderAsync(
        topic, startMessageId->messageId, conf->conf,
        std::bind(&handle_reader_callback, std::placeholders::_1, std::placeholders::_2, callback, ctx));
}

pulsar_result pulsar_client_get_topic_partitions(pulsar_client_t *client, const char *topic,
                                                 pulsar_string_list_t **partitions) {
    std::vector<std::string> partitionsList;
    pulsar::Result res = client->client->getPartitionsForTopic(topic, partitionsList);
    if (res == pulsar::ResultOk) {
        (*partitions) = pulsar_string_list_create();

        for (int i = 0; i < partitionsList.size(); i++) {
            pulsar_string_list_append(*partitions, partitionsList[i].c_str());
        }

        return pulsar_result_Ok;
    } else {
        return (pulsar_result)res;
    }
}

static void handle_get_partitions_callback(pulsar::Result result,
                                           const std::vector<std::string> &partitionsList,
                                           pulsar_get_partitions_callback callback, void *ctx) {
    if (result == pulsar::ResultOk) {
        pulsar_string_list_t *partitions = pulsar_string_list_create();

        for (int i = 0; i < partitionsList.size(); i++) {
            pulsar_string_list_append(partitions, partitionsList[i].c_str());
        }

        callback((pulsar_result)result, partitions, ctx);
    } else {
        callback((pulsar_result)result, NULL, ctx);
    }
}

void pulsar_client_get_topic_partitions_async(pulsar_client_t *client, const char *topic,
                                              pulsar_get_partitions_callback callback, void *ctx) {
    client->client->getPartitionsForTopicAsync(
        topic, std::bind(&handle_get_partitions_callback, std::placeholders::_1, std::placeholders::_2,
                         callback, ctx));
}

pulsar_result pulsar_client_close(pulsar_client_t *client) { return (pulsar_result)client->client->close(); }

static void handle_client_close(pulsar::Result result, pulsar_close_callback callback, void *ctx) {
    callback((pulsar_result)result, ctx);
}

void pulsar_client_close_async(pulsar_client_t *client, pulsar_close_callback callback, void *ctx) {
    client->client->closeAsync(std::bind(handle_client_close, std::placeholders::_1, callback, ctx));
}
