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
#include <pulsar/c/producer_configuration.h>
#include <gtest/gtest.h>

TEST(C_ProducerConfigurationTest, testCApiConfig) {
    pulsar_producer_configuration_t *producer_conf = pulsar_producer_configuration_create();

    ASSERT_EQ(pulsar_producer_configuration_is_chunking_enabled(producer_conf), 0);
    pulsar_producer_configuration_set_chunking_enabled(producer_conf, 1);
    ASSERT_EQ(pulsar_producer_configuration_is_chunking_enabled(producer_conf), 1);
}
