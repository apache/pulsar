package org.apache.pulsar.broker.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.nio.charset.StandardCharsets;

@Slf4j
@Test(groups = "broker-admin")
public class TopicPoliciesWithBrokerRestartTest extends MockedPulsarServiceBaseTest {

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test
    public void testRetentionWithBrokerRestart() throws Exception {
        final int messages = 1_000;
        // (1) Init topic
        final String topicName = "persistent://public/default/retention_with_broker_restart";
        admin.topics().createNonPartitionedTopic(topicName);
        // (2) Set retention
        final RetentionPolicies retentionPolicies = new RetentionPolicies(20, 20);
        admin.topicPolicies().setRetention(topicName, retentionPolicies);
        // (3) Send messages
        final Producer<byte[]> publisher = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        for (int i = 0; i < messages; i++) {
            publisher.send((i + "").getBytes(StandardCharsets.UTF_8));
        }
        // (4) Check ledger Id
        final PersistentTopicInternalStats internalStats1 = admin.topics().getInternalStats(topicName);
        final String lastConfirmedEntry1 = internalStats1.lastConfirmedEntry;
        // (5) Restart broker
        restartBroker();
        // (6) Check topic again
        final PersistentTopicInternalStats internalStats2 = admin.topics().getInternalStats(topicName);
        final String lastConfirmedEntry2 = internalStats2.lastConfirmedEntry;
        Assert.assertEquals(lastConfirmedEntry2, lastConfirmedEntry1);
    }
}
