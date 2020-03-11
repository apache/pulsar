package org.apache.pulsar.broker.namespace;


import com.google.common.collect.Sets;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnerShipCacheForCurrentServer extends OwnerShipForCurrentServerTestBase {

    private final static String TENANT = "ownership";
    private final static String NAMESPACE = TENANT + "/ns1";
    private final static String TOPIC_TEST = NAMESPACE + "/test";

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();
        admin.tenants().createTenant(TENANT,
                new TenantInfo(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE);
        admin.topics().createNonPartitionedTopic(TOPIC_TEST);
    }

    @Test
    public void testOwnershipForCurrentServer() throws Exception {
        NamespaceService namespaceServiceZero = getPulsarServiceList().get(0).getNamespaceService();
        NamespaceService namespaceServiceOne = getPulsarServiceList().get(1).getNamespaceService();
        NamespaceBundle bundle = namespaceServiceZero.getBundle(TopicName.get(TOPIC_TEST));
        namespaceServiceZero.registerNamespace(NAMESPACE, true);
        String nativeUrlFirst = namespaceServiceOne.getOwnerAsync(bundle).get().get().getNativeUrl();
        String nativeUrlTwice = namespaceServiceOne.getOwnerAsync(bundle).get().get().getNativeUrl();
    }

}
