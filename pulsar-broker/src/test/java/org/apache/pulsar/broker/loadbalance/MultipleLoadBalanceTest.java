package org.apache.pulsar.broker.loadbalance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class MultipleLoadBalanceTest extends MultiBrokerBaseTest {

    @DataProvider(name = "loadManagerName")
    public static Object[][] loadManagerName() {
        return new Object[][]{
                {ModularLoadManagerImpl.class.getName()},
                {SimpleLoadManagerImpl.class.getName()}
        };
    }

    // 1. Create 100 common topic, 10 system topic and send same message to these topics to let them have same usage.
    // 2. Find a common topic and a system topic that localed at the same broker.
    // 3. Increase the overload of the topics (Find in step 2).
    // 4. Do load shedding.
    // 5. Verify the ownership of the common topic has change, but the system doesn't.
    @Test(dataProvider = "loadManagerName")
    public void testDoNoTUnloadBundleForSystemNameSpace(String loadManagerName) throws Exception {
        // 0. Prepare environment
        List<PulsarService> allPulsarService = getAllBrokers();
        for (int i = 0; i < allPulsarService.size(); i++) {
            ServiceConfiguration conf = allPulsarService.get(i).getConfig();
            conf.setLoadManagerClassName(loadManagerName);
            conf.setLoadBalancerSheddingEnabled(true);
            conf.setSystemTopicEnabled(true);
            conf.setAllowAutoTopicCreation(true);
            conf.setLoadBalancerSheddingGracePeriodMinutes(5);
            conf.setLoadBalancerBrokerThresholdShedderPercentage(-100);
            conf.setLoadBalancerBundleUnloadMinThroughputThreshold(-100);
            conf.setLoadBalancerReportUpdateMinIntervalMillis(1);
        }

        PulsarAdmin admin = pulsar.getAdminClient();
        admin.tenants().createTenant("prop",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        String namespace = "public/test/prop";
        String topic = "persistent://" + namespace + "/testDoNoTUnloadBundleForSystemTopic-";
        String systemTopic = "persistent://" + NamespaceName.SYSTEM_NAMESPACE.getNamespaceObject().toString()
                + "/testDoNoTUnloadBundleForSystemTopic-";
        BundlesData bundleData = BundlesData.builder().numBundles(10).build();
        admin.namespaces().createNamespace(namespace, bundleData);
        admin.tenants().createTenant("pulsar",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.getNamespaceObject().toString(), bundleData);
        int totalTopics = 100;
        int totalSystemTopics = 10;
        // 1. Create 100 common topic, 10 system topic and send same message to these topics to
        // let them have same usage.
        for (int i = 0; i < totalTopics; i++) {
            @Cleanup
            Producer<byte[]> producer = pulsarClient.newProducer()
                    .topic(topic + i)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .create();
            for (int j = 0; j < 10; j++) {
                producer.newMessage().value("message".getBytes()).sendAsync();
            }
        }
        for (int i = 0; i < totalSystemTopics; i++) {
            @Cleanup
            Producer<byte[]> producer = pulsarClient.newProducer().topic(systemTopic + i)
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .create();
            for (int j = 0; j < 10; j++) {
                producer.newMessage().value("message".getBytes()).sendAsync();
            }
        }
        // 2. Find a common topic and a system topic that localed at the same broker.
        PulsarService owner = null;
        ArrayList<TopicName> topicNames = new ArrayList<>();
        for (int i = 0; i < allPulsarService.size(); i++) {
            if (allPulsarService.get(i).getNamespaceService().checkTopicOwnership(TopicName.get(systemTopic + "0"))
                    .get()) {
                owner = allPulsarService.get(i);
                for (int j = 0; j < totalTopics; j++) {
                    if (owner.getNamespaceService().checkTopicOwnership(TopicName.get(topic + j)).get()) {
                        topicNames.add(TopicName.get(systemTopic + "0"));
                        topicNames.add(TopicName.get(topic + j));
                        break;
                    }
                }
                break;
            }
        }
        assertEquals(topicNames.size(), 2);
        assertNotNull(owner);
        // 3. Increase the overload of the topics (Find in step 2).
        topicNames.forEach(topicName -> {
            try {
                Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName.toString())
                        .sendTimeout(0, TimeUnit.SECONDS)
                        .create();
                for (int i = 0; i < 1000; i++) {
                    producer.newMessage().value("message".getBytes()).send();
                }
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
        // 4. Do load shedding.
        LoadManagerReport loadManagerReport = admin.brokerStats().getLoadReport();
        Field loadManageField = PulsarService.class.getDeclaredField("loadManager");
        loadManageField.setAccessible(true);
        AtomicReference<LoadManager> loadManagerAtomicReference =
                (AtomicReference<LoadManager>) loadManageField.get(owner);
        loadManagerAtomicReference.get().doLoadShedding();
        // 5. Verify the ownership of the common topic has change, but ownership the system topic doesn't change.
        assertTrue(owner.getNamespaceService().checkTopicOwnership(topicNames.get(0)).get());
        assertFalse(owner.getNamespaceService().checkTopicOwnership(topicNames.get(1)).get());
    }
}
