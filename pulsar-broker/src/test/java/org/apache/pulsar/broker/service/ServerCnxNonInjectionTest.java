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

package org.apache.pulsar.broker.service;

import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.ServerError;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class ServerCnxNonInjectionTest extends ProducerConsumerBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 60 * 1000)
    public void testCheckConnectionLivenessAfterClosed() throws Exception {
        // Create a ServerCnx
        final String tp = BrokerTestUtil.newUniqueName("public/default/tp");
        Producer<String> p = pulsarClient.newProducer(Schema.STRING).topic(tp).create();
        ServerCnx serverCnx = (ServerCnx) pulsar.getBrokerService().getTopic(tp, false).join().get()
                        .getProducers().values().iterator().next().getCnx();
        // Call "CheckConnectionLiveness" after serverCnx is closed. The resulted future should be done eventually.
        p.close();
        serverCnx.close();
        Thread.sleep(1000);
        serverCnx.checkConnectionLiveness().join();
    }

    @Test
    public void producerPersistErrorTest() throws Exception {
        PulsarClient client = PulsarClient.builder().serviceUrl(lookupUrl.toString())
                .maxNumberOfRejectedRequestPerConnection(1000).build();

        // Create a producer with customized name.
        final String tp = BrokerTestUtil.newUniqueName("public/default/tp");
        admin.topics().createNonPartitionedTopic(tp);
        Producer<String> p = client.newProducer(Schema.STRING).producerName("p1").topic(tp).create();

        // Inject a persistence error.
        org.apache.pulsar.broker.service.Producer serverProducer = pulsar.getBrokerService().getTopic(tp, false)
                .join().get().getProducers().values().iterator().next();
        ServerCnx serverCnx = (ServerCnx) serverProducer.getCnx();
        serverCnx.getCommandSender().sendSendError(serverProducer.getProducerId(), 1/* sequenceId */,
                ServerError.PersistenceError, "mocked error");
        // Wait for the client receives the error.
        Thread.sleep(1000);

        try {
            // Verify: the next publish will finish.
            MessageId messageId = p.sendAsync("1").get(10, TimeUnit.SECONDS);
            MessageIdAdv messageIdAdv = (MessageIdAdv) messageId;
            log.info("sent {}:{}", messageIdAdv.getLedgerId(), messageIdAdv.getEntryId());
        } finally {
            // cleanup orphan producers.
            serverCnx.ctx().close();
            // cleanup
            client.close();
            p.close();
            admin.topics().delete(tp);
        }
    }

}
