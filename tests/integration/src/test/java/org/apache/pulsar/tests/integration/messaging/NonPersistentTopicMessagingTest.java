/*
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
package org.apache.pulsar.tests.integration.messaging;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class NonPersistentTopicMessagingTest extends MessagingBase {

    TopicMessagingTest test;

    @BeforeClass(alwaysRun = true)
    public void setupTest() throws Exception {
        this.test = new TopicMessagingTest(getPulsarClient(), getPulsarAdmin());
    }

    @Test
    public void testNonPartitionedTopicMessagingWithExclusive() throws Exception {
        test.nonPartitionedTopicSendAndReceiveWithExclusive(false);
    }

    @Test
    public void testPartitionedTopicMessagingWithExclusive() throws Exception {
        test.partitionedTopicSendAndReceiveWithExclusive(false);
    }

    @Test
    public void testNonPartitionedTopicMessagingWithFailover() throws Exception {
        test.nonPartitionedTopicSendAndReceiveWithFailover(false);
    }

    @Test
    public void testPartitionedTopicMessagingWithFailover() throws Exception {
        test.partitionedTopicSendAndReceiveWithFailover(false);
    }

    @Test
    public void testNonPartitionedTopicMessagingWithShared() throws Exception {
        test.nonPartitionedTopicSendAndReceiveWithShared(false);
    }

    @Test
    public void testPartitionedTopicMessagingWithShared() throws Exception {
        test.partitionedTopicSendAndReceiveWithShared(false);
    }

    @Test
    public void testNonPartitionedTopicMessagingWithKeyShared() throws Exception {
        test.nonPartitionedTopicSendAndReceiveWithKeyShared(false);
    }

    @Test
    public void testPartitionedTopicMessagingWithKeyShared() throws Exception {
        test.partitionedTopicSendAndReceiveWithKeyShared(false);
    }
}
