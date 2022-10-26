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
package org.apache.pulsar.broker.service.persistent;


import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ShadowManagedLedgerImpl;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ShadowTopicTest extends BrokerTestBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test()
    public void testNonPartitionedShadowTopicSetup() throws Exception {
        String sourceTopic = "persistent://prop/ns-abc/source";
        String shadowTopic = "persistent://prop/ns-abc/shadow";
        //1. test shadow topic setting in topic creation.
        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        PersistentTopic brokerShadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopic).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopic);
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);

        //2. test shadow topic could be properly loaded after unload.
        admin.namespaces().unload("prop/ns-abc");
        Assert.assertTrue(pulsar.getBrokerService().getTopicReference(shadowTopic).isEmpty());
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);
        brokerShadowTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopic).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopic);
    }

    @Test()
    public void testPartitionedShadowTopicSetup() throws Exception {
        String sourceTopic = "persistent://prop/ns-abc/source-p";
        String shadowTopic = "persistent://prop/ns-abc/shadow-p";
        String shadowTopicPartition = TopicName.get(shadowTopic).getPartition(0).toString();

        //1. test shadow topic setting in topic creation.
        admin.topics().createPartitionedTopic(sourceTopic, 2);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        pulsarClient.newProducer().topic(shadowTopic).create().close();//trigger loading partitions.
        PersistentTopic brokerShadowTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(shadowTopicPartition).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopic);
        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);

        //2. test shadow topic could be properly loaded after unload.
        admin.namespaces().unload("prop/ns-abc");
        Assert.assertTrue(pulsar.getBrokerService().getTopicReference(shadowTopic).isEmpty());

        Assert.assertEquals(admin.topics().getShadowSource(shadowTopic), sourceTopic);
        brokerShadowTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(shadowTopicPartition).get().get();
        Assert.assertTrue(brokerShadowTopic.getManagedLedger() instanceof ShadowManagedLedgerImpl);
        Assert.assertEquals(brokerShadowTopic.getShadowSourceTopic().get().toString(), sourceTopic);
    }


}
