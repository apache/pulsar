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
package org.apache.pulsar.client.impl;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class DisabledSystemTopicTest extends ProducerConsumerBase {

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTransactionCoordinatorEnabled(false);
        conf.setSystemTopicEnabled(false);
    }

    @Test
    public void testDeleteTopic() throws Exception {
        String topicName = "persistent://my-property/my-ns/tp_" + UUID.randomUUID().toString();

        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().delete(topicName, false);

        admin.topics().createPartitionedTopic(topicName, 3);
        admin.topics().deletePartitionedTopic(topicName);
    }
}
