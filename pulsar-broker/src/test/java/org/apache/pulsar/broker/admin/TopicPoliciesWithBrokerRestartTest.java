package org.apache.pulsar.broker.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.mledger.util.Futures.NULL_PROMISE;

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
        admin.namespaces().createNamespace("public/retention");
        final String topicName = "persistent://public/retention/retention_with_broker_restart";
        admin.topics().createNonPartitionedTopic(topicName);
        for (int i = 0; i < 500; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            admin.topics().createNonPartitionedTopic(shadowTopicNames);
        }
        // (2) Set retention
        final RetentionPolicies retentionPolicies = new RetentionPolicies(20, 20);
        for (int i = 0; i < 500; i++) {
            final String shadowTopicNames = topicName + "_" + i;
            admin.topicPolicies().setRetention(shadowTopicNames, retentionPolicies);
        }
        admin.topicPolicies().setRetention(topicName, retentionPolicies);
        // (3) Send messages
        final Producer<byte[]> publisher = pulsarClient.newProducer()
                .topic(topicName)
                .create();
        for (int i = 0; i < messages; i++) {
            publisher.send((i + "").getBytes(StandardCharsets.UTF_8));
        }
        // (4) Check configuration
        Awaitility.await().untilAsserted(() -> {
            PersistentTopic persistentTopic1 = (PersistentTopic)
                    pulsar.getBrokerService().getTopic(topicName, true).join().get();
            ManagedLedgerImpl managedLedger1 = (ManagedLedgerImpl) persistentTopic1.getManagedLedger();
            Assert.assertEquals(managedLedger1.getConfig().getRetentionSizeInMB(), 20);
            Assert.assertEquals(managedLedger1.getConfig().getRetentionTimeMillis(),
                    TimeUnit.MINUTES.toMillis(20));
        });
        // (5) Restart broker
        restartBroker();
        // (6) Check configuration again
        admin.lookups().lookupTopic(topicName);
        PersistentTopic persistentTopic2 = (PersistentTopic)
                pulsar.getBrokerService().getTopic(topicName, true).join().get();
        ManagedLedgerImpl managedLedger2 = (ManagedLedgerImpl) persistentTopic2.getManagedLedger();
        Assert.assertEquals(managedLedger2.getConfig().getRetentionSizeInMB(), 20);
        Assert.assertEquals(managedLedger2.getConfig().getRetentionTimeMillis(),
                TimeUnit.MINUTES.toMillis(20));
    }
}
