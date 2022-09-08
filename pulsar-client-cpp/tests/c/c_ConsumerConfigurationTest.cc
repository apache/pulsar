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
#include <pulsar/c/consumer_configuration.h>
#include <gtest/gtest.h>

TEST(C_ConsumerConfigurationTest, testCApiConfig) {
    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();

    ASSERT_EQ(pulsar_consumer_configuration_get_max_pending_chunked_message(consumer_conf), 10);
    pulsar_consumer_configuration_set_max_pending_chunked_message(consumer_conf, 100);
    ASSERT_EQ(pulsar_consumer_configuration_get_max_pending_chunked_message(consumer_conf), 100);

    ASSERT_EQ(pulsar_consumer_configuration_is_auto_ack_oldest_chunked_message_on_queue_full(consumer_conf),
              0);
    pulsar_consumer_configuration_set_auto_ack_oldest_chunked_message_on_queue_full(consumer_conf, 1);
    ASSERT_EQ(pulsar_consumer_configuration_is_auto_ack_oldest_chunked_message_on_queue_full(consumer_conf),
              1);
}
