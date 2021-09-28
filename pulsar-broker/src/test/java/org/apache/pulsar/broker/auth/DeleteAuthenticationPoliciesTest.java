package org.apache.pulsar.broker.auth;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DeleteAuthenticationPoliciesTest extends MockedPulsarServiceBaseTest {

    public DeleteAuthenticationPoliciesTest() {
        super();
    }

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setClusterName("c1");

        internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        internalCleanup();
    }
    @Test
    public void testDeleteAuthenticationPoliciesOfTopic() throws PulsarAdminException, MetadataStoreException {
        admin.clusters().createCluster("c1", ClusterData.builder().build());
        admin.tenants().createTenant("p1",
                new TenantInfoImpl(Collections.emptySet(), new HashSet<>(admin.clusters().getClusters())));
        waitForChange();
        admin.namespaces().createNamespace("p1/ns1");

        // test for non-partitioned topic
        String topic = "persistent://p1/ns1/topic";
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().grantPermission(topic, "test-user", EnumSet.of(AuthAction.consume));
        waitForChange();

        assertTrue(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get("p1/ns1"))
                .get().auth_policies.getTopicAuthentication().containsKey(topic));

        admin.topics().delete(topic);
        waitForChange();

        assertFalse(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get("p1/ns1"))
                .get().auth_policies.getTopicAuthentication().containsKey(topic));

        // test for partitioned topic
        String partitionedTopic = "persistent://p1/ns1/partitioned-topic";
        int numPartitions = 5;

        admin.topics().createPartitionedTopic(partitionedTopic, numPartitions);
        admin.topics()
                .grantPermission(partitionedTopic, "test-user", EnumSet.of(AuthAction.consume));
        waitForChange();

        assertTrue(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get("p1/ns1"))
                .get().auth_policies.getTopicAuthentication().containsKey(partitionedTopic));

        for (int i = 0; i < numPartitions; i++) {
            assertTrue(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get("p1/ns1"))
                    .get().auth_policies.getTopicAuthentication()
                    .containsKey(TopicName.get(partitionedTopic).getPartition(i).toString()));
        }

        admin.topics().deletePartitionedTopic("persistent://p1/ns1/partitioned-topic");
        waitForChange();

        assertFalse(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get("p1/ns1"))
                .get().auth_policies.getTopicAuthentication().containsKey(partitionedTopic));
        for (int i = 0; i < numPartitions; i++) {
            assertFalse(pulsar.getPulsarResources().getNamespaceResources().getPolicies(NamespaceName.get("p1/ns1"))
                    .get().auth_policies.getTopicAuthentication()
                    .containsKey(TopicName.get(partitionedTopic).getPartition(i).toString()));
        }

        admin.namespaces().deleteNamespace("p1/ns1");
        admin.tenants().deleteTenant("p1");
        admin.clusters().deleteCluster("c1");

    }

    private static void waitForChange() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
    }
}
