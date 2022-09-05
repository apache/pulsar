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

#include <gtest/gtest.h>
#include <pulsar/c/client.h>
#include <pulsar/c/producer.h>
#include <pulsar/c/consumer.h>
#include <pulsar/c/client_configuration.h>
#include <pulsar/c/consumer_configuration.h>
#include <pulsar/c/message.h>
#include <pulsar/c/message_id.h>
#include <pulsar/c/result.h>

#include "../lib/Future.h"

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
    pulsar::Promise<pulsar_result, pulsar_message_id_t *> send_promise;
    const char *content = "msg-1-content";
    pulsar_message_t *msg = pulsar_message_create();
    pulsar_message_set_content(msg, content, strlen(content));
    ASSERT_STREQ("(-1,-1,-1,-1)", pulsar_message_id_str(pulsar_message_get_message_id(msg)));
    pulsar_producer_send_async(
        producer, msg,
        [](pulsar_result async_result, pulsar_message_id_t *msg_id, void *ctx) {
            auto ctx_promise = static_cast<pulsar::Promise<pulsar_result, pulsar_message_id_t *> *>(ctx);
            ASSERT_EQ(pulsar_result_Ok, async_result);
            ctx_promise->setValue(msg_id);
        },
        &send_promise);

    pulsar_message_id_t *msg_id;
    send_promise.getFuture().get(msg_id);
    ASSERT_STRNE("(-1,-1,-1,-1)", pulsar_message_id_str(msg_id));
    pulsar_message_id_free(msg_id);
    pulsar_message_free(msg);

    pulsar::Promise<pulsar_result, pulsar_message_t *> receive_promise;
    // receive asynchronously
    pulsar_consumer_receive_async(
        consumer,
        [](pulsar_result async_result, pulsar_message_t *received_msg, void *ctx) {
            auto ctx_promise = static_cast<pulsar::Promise<pulsar_result, pulsar_message_t *> *>(ctx);
            ASSERT_EQ(pulsar_result_Ok, async_result);
            ctx_promise->setValue(received_msg);
        },
        &receive_promise);

    pulsar_message_t *received_msg;
    receive_promise.getFuture().get(received_msg);
    ASSERT_STREQ(content, static_cast<const char *>(pulsar_message_get_data(received_msg)));
    pulsar_message_free(received_msg);

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
