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

import java.lang.reflect.Method;
import java.util.Set;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PropertyAdmin;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public abstract class ProducerConsumerBase extends MockedPulsarServiceBaseTest {
    protected String methodName;

    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    public void producerBaseSetup() throws Exception {
        admin.clusters().createCluster("use", new ClusterData("http://127.0.0.1:" + BROKER_WEBSERVICE_PORT));
        admin.properties().createProperty("my-property",
                new PropertyAdmin(Lists.newArrayList("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");
    }

    protected void testMessageOrderAndDuplicates(Set<String> messagesReceived, String receivedMessage,
            String expectedMessage) {
        // Make sure that messages are received in order
        Assert.assertEquals(receivedMessage, expectedMessage,
                "Received message " + receivedMessage + " did not match the expected message " + expectedMessage);

        // Make sure that there are no duplicates
        Assert.assertTrue(messagesReceived.add(receivedMessage), "Received duplicate message " + receivedMessage);
    }
}
