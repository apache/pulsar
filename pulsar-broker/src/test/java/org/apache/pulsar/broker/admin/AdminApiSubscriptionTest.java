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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;
import java.util.UUID;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class AdminApiSubscriptionTest extends MockedPulsarServiceBaseTest {
    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testExpireMessageWithNonExistTopicAndExistSub() throws Exception {
        String uuid = UUID.randomUUID().toString();
        String topic = "test-expire-messages-non-exist-topic-" + uuid;
        String subscriptionName = "test-expire-messages-sub-" + uuid;

        admin.topics().createSubscription(topic, subscriptionName, MessageId.latest);

        assertEquals(expectThrows(PulsarAdminException.class,
                        () -> admin.topics().expireMessages(topic, subscriptionName, 1)).getStatusCode(),
                Response.Status.CONFLICT.getStatusCode());
        assertEquals(expectThrows(PulsarAdminException.class,
                        () -> admin.topics().expireMessagesForAllSubscriptions(topic, 1)).getStatusCode(),
                Response.Status.CONFLICT.getStatusCode());
    }

    @Test
    public void testExpireMessageWithNonExistTopicAndNonExistSub() {
        String uuid = UUID.randomUUID().toString();
        String topic = "test-expire-messages-non-exist-topic-" + uuid;
        String subscriptionName = "test-expire-messages-non-exist-sub-" + uuid;

        PulsarAdminException exception = expectThrows(PulsarAdminException.class,
                () -> admin.topics().expireMessages(topic, subscriptionName, 1));
        assertEquals(exception.getStatusCode(), Response.Status.NOT_FOUND.getStatusCode());
        assertEquals(exception.getMessage(), "Topic not found");

        exception = expectThrows(PulsarAdminException.class,
                () -> admin.topics().expireMessagesForAllSubscriptions(topic, 1));
        assertEquals(exception.getStatusCode(), Response.Status.NOT_FOUND.getStatusCode());
        assertEquals(exception.getMessage(), "Topic not found");
    }

    @Test
    public void tesSkipMessageWithNonExistTopicAndExistSub() throws Exception {
        String uuid = UUID.randomUUID().toString();
        String topic = "test-skip-messages-non-exist-topic-" + uuid;
        String subscriptionName = "test-skip-messages-sub-" + uuid;
        admin.topics().createSubscription(topic, subscriptionName, MessageId.latest);

        admin.topics().skipMessages(topic, subscriptionName, 1);
        admin.topics().skipAllMessages(topic, subscriptionName);
    }

    @Test
    public void tesSkipMessageWithNonExistTopicAndNotExistSub() {
        String uuid = UUID.randomUUID().toString();
        String topic = "test-skip-messages-non-exist-topic-" + uuid;
        String subscriptionName = "test-skip-messages-non-exist-sub-" + uuid;

        PulsarAdminException exception = expectThrows(PulsarAdminException.class,
                () -> admin.topics().skipMessages(topic, subscriptionName, 1));
        assertEquals(exception.getStatusCode(), Response.Status.NOT_FOUND.getStatusCode());
        assertEquals(exception.getMessage(), "Topic not found");

        exception = expectThrows(PulsarAdminException.class,
                () -> admin.topics().skipAllMessages(topic, subscriptionName));
        assertEquals(exception.getStatusCode(), Response.Status.NOT_FOUND.getStatusCode());
        assertEquals(exception.getMessage(), "Topic not found");
    }
}
