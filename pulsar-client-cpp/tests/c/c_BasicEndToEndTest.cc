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

#include <future>
#include <stdlib.h>
#include <string.h>

#include <gtest/gtest.h>
#include <pulsar/c/client.h>

struct send_ctx {
    pulsar_result result;
    char *msg_id;
    std::promise<void> *promise;
};

struct receive_ctx {
    pulsar_result result;
    pulsar_consumer_t *consumer;
    char *data;
    std::promise<void> *promise;
};

static void send_callback(pulsar_result async_result, pulsar_message_id_t *msg_id, void *ctx) {
    struct send_ctx *send_ctx = (struct send_ctx *)ctx;
    send_ctx->result = async_result;
    if (async_result == pulsar_result_Ok) {
        const char *msg_id_str = pulsar_message_id_str(msg_id);
        send_ctx->msg_id = (char *)malloc(strlen(msg_id_str) * sizeof(char));
        strcpy(send_ctx->msg_id, msg_id_str);
    }
    send_ctx->promise->set_value();
    pulsar_message_id_free(msg_id);
}

static void receive_callback(pulsar_result async_result, pulsar_message_t *msg, void *ctx) {
    struct receive_ctx *receive_ctx = (struct receive_ctx *)ctx;
    receive_ctx->result = async_result;
    if (async_result == pulsar_result_Ok &&
        pulsar_consumer_acknowledge(receive_ctx->consumer, msg) == pulsar_result_Ok) {
        const char *data = (const char *)pulsar_message_get_data(msg);
        receive_ctx->data = (char *)malloc(strlen(data) * sizeof(char));
        strcpy(receive_ctx->data, data);
    }
    receive_ctx->promise->set_value();
    pulsar_message_free(msg);
}

TEST(c_BasicEndToEndTest, testAsyncProduceConsume) {
    const char *lookup_url = "pulsar://localhost:6650";
    const char *topic_name = "persistent://public/default/test-c-produce-consume";
    const char *sub_name = "my-sub-name";

    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create(lookup_url, conf);

    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_t *producer;
    pulsar_result result = pulsar_client_create_producer(client, topic_name, producer_conf, &producer);
    ASSERT_EQ(pulsar_result_Ok, result);

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    pulsar_consumer_t *consumer;
    result = pulsar_client_subscribe(client, topic_name, sub_name, consumer_conf, &consumer);
    ASSERT_EQ(pulsar_result_Ok, result);

    ASSERT_STREQ(topic_name, pulsar_producer_get_topic(producer));
    ASSERT_STREQ(topic_name, pulsar_consumer_get_topic(consumer));
    ASSERT_STREQ(sub_name, pulsar_consumer_get_subscription_name(consumer));

    // send asynchronously
    std::promise<void> send_promise;
    std::future<void> send_future = send_promise.get_future();
    struct send_ctx send_ctx = {pulsar_result_UnknownError, NULL, &send_promise};
    const char *content = "msg-1-content";
    pulsar_message_t *msg = pulsar_message_create();
    pulsar_message_set_content(msg, content, strlen(content));
    ASSERT_STREQ("(-1,-1,-1,-1)", pulsar_message_id_str(pulsar_message_get_message_id(msg)));
    pulsar_producer_send_async(producer, msg, send_callback, &send_ctx);
    send_future.get();
    ASSERT_EQ(pulsar_result_Ok, send_ctx.result);
    ASSERT_STRNE("(-1,-1,-1,-1)", send_ctx.msg_id);
    delete send_ctx.msg_id;

    // receive asynchronously
    std::promise<void> receive_promise;
    std::future<void> receive_future = receive_promise.get_future();
    struct receive_ctx receive_ctx = {pulsar_result_UnknownError, consumer, NULL, &receive_promise};
    pulsar_consumer_receive_async(consumer, receive_callback, &receive_ctx);
    receive_future.get();
    ASSERT_EQ(pulsar_result_Ok, receive_ctx.result);
    ASSERT_STREQ(content, receive_ctx.data);
    delete receive_ctx.data;

    ASSERT_EQ(pulsar_result_Ok, pulsar_consumer_unsubscribe(consumer));
    ASSERT_EQ(pulsar_result_AlreadyClosed, pulsar_consumer_close(consumer));
    ASSERT_EQ(pulsar_result_Ok, pulsar_producer_close(producer));
    ASSERT_EQ(pulsar_result_Ok, pulsar_client_close(client));

    pulsar_consumer_free(consumer);
    pulsar_consumer_configuration_free(consumer_conf);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
}
