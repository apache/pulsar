package org.apache.pulsar.broker.namespace;


import com.google.common.collect.Sets;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnerShipCacheForCurrentServerTest extends OwnerShipForCurrentServerTestBase {

    private final static String TENANT = "ownership";
    private final static String NAMESPACE = TENANT + "/ns1";
    private final static String TOPIC_TEST = NAMESPACE + "/test";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();
        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData("http://localhost:" + webServicePort));
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE);
        admin.topics().createNonPartitionedTopic(TOPIC_TEST);
    }

    @AfterMethod
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testOwnershipForCurrentServer() throws Exception {
        NamespaceService[] namespaceServices = new NamespaceService[getPulsarServiceList().size()];
        for (int i = 0; i < getPulsarServiceList().size(); i++) {
            namespaceServices[i] = getPulsarServiceList().get(i).getNamespaceService();
            NamespaceBundle bundle = namespaceServices[i].getBundle(TopicName.get(TOPIC_TEST));
            Assert.assertEquals(namespaceServices[i].getOwnerAsync(bundle).get().get().getNativeUrl(),
                    namespaceServices[i].getOwnerAsync(bundle).get().get().getNativeUrl());
        }
    }
}
