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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.testng.annotations.Test;

import org.apache.pulsar.client.impl.ConsumerId;

import java.util.Objects;

public class ConsumerIdTest {
    private static final String TOPIC_TEST = "my-topic-1";
    private static final String TOPIC_TEST_2 = "my-topic-2";
    private static final String SUBSCRIPTION_TEST = "my-sub-1";

    @Test
    public void getTopicTest() {
        ConsumerId testConsumerId = new ConsumerId(TOPIC_TEST, SUBSCRIPTION_TEST);
        assertEquals(TOPIC_TEST, testConsumerId.getTopic());
    }

    @Test
    public void getSubscriptionTest() {
        ConsumerId testConsumerId = new ConsumerId(TOPIC_TEST, SUBSCRIPTION_TEST);
        assertEquals(SUBSCRIPTION_TEST, testConsumerId.getSubscription());
    }

    @Test
    public void hashCodeTest() {
        ConsumerId testConsumerId = new ConsumerId(TOPIC_TEST, SUBSCRIPTION_TEST);
        assertEquals(Objects.hash(TOPIC_TEST, SUBSCRIPTION_TEST), testConsumerId.hashCode());
    }

    @Test
    public void equalTest() {
        ConsumerId testConsumerId1 = new ConsumerId(TOPIC_TEST, SUBSCRIPTION_TEST);
        ConsumerId testConsumerId2 = new ConsumerId(TOPIC_TEST, SUBSCRIPTION_TEST);
        ConsumerId testConsumerId3 = new ConsumerId(TOPIC_TEST_2, SUBSCRIPTION_TEST);

        assertEquals(testConsumerId2, testConsumerId1);

        assertNotEquals(testConsumerId3, testConsumerId1);

        assertNotEquals("", testConsumerId1);
    }

    @Test
    public void compareToTest() {
        ConsumerId testConsumerId1 = new ConsumerId(TOPIC_TEST, SUBSCRIPTION_TEST);
        ConsumerId testConsumerId2 = new ConsumerId(TOPIC_TEST, SUBSCRIPTION_TEST);
        ConsumerId testConsumerId3 = new ConsumerId(TOPIC_TEST_2, SUBSCRIPTION_TEST);

        assertEquals(0, testConsumerId1.compareTo(testConsumerId2));
        assertEquals(-1, testConsumerId1.compareTo(testConsumerId3));

    }
}
