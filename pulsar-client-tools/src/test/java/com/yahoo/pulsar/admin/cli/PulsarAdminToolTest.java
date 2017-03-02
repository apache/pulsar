/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.admin.cli;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.util.EnumSet;

import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.yahoo.pulsar.client.admin.Brokers;
import com.yahoo.pulsar.client.admin.Clusters;
import com.yahoo.pulsar.client.admin.Lookup;
import com.yahoo.pulsar.client.admin.Namespaces;
import com.yahoo.pulsar.client.admin.PersistentTopics;
import com.yahoo.pulsar.client.admin.Properties;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.admin.ResourceQuotas;
import com.yahoo.pulsar.common.policies.data.AuthAction;
import com.yahoo.pulsar.common.policies.data.BacklogQuota;
import com.yahoo.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.PersistencePolicies;
import com.yahoo.pulsar.common.policies.data.PropertyAdmin;
import com.yahoo.pulsar.common.policies.data.ResourceQuota;
import com.yahoo.pulsar.common.policies.data.RetentionPolicies;

@Test
public class PulsarAdminToolTest {
    @Test
    void brokers() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Brokers mockBrokers = mock(Brokers.class);
        doReturn(mockBrokers).when(admin).brokers();

        CmdBrokers brokers = new CmdBrokers(admin);

        brokers.run(split("list use"));
        verify(mockBrokers).getActiveBrokers("use");
        
        brokers.run(split("get-all-dynamic-config"));
        verify(mockBrokers).getAllDynamicConfigurations();
        
        brokers.run(split("list-dynamic-config"));
        verify(mockBrokers).getDynamicConfigurationNames();
        
        brokers.run(split("update-dynamic-config --config brokerShutdownTimeoutMs --value 100"));
        verify(mockBrokers).updateDynamicConfiguration("brokerShutdownTimeoutMs", "100");
    }

    @Test
    void getOwnedNamespaces() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Brokers mockBrokers = mock(Brokers.class);
        doReturn(mockBrokers).when(admin).brokers();

        CmdBrokers brokers = new CmdBrokers(admin);

        brokers.run(split("namespaces use --url http://my-service.url:4000"));
        verify(mockBrokers).getOwnedNamespaces("use", "http://my-service.url:4000");

    }

    @Test
    void clusters() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Clusters mockClusters = mock(Clusters.class);
        when(admin.clusters()).thenReturn(mockClusters);

        CmdClusters clusters = new CmdClusters(admin);

        clusters.run(split("list"));
        verify(mockClusters).getClusters();

        clusters.run(split("get use"));
        verify(mockClusters).getCluster("use");

        clusters.run(split("create use --url http://my-service.url:8080"));
        verify(mockClusters).createCluster("use", new ClusterData("http://my-service.url:8080", null));

        clusters.run(split("update use --url http://my-service.url:8080"));
        verify(mockClusters).updateCluster("use", new ClusterData("http://my-service.url:8080", null));

        clusters.run(split("delete use"));
        verify(mockClusters).deleteCluster("use");

        // Re-create CmdClusters to avoid a issue.
        // See https://github.com/cbeust/jcommander/issues/271
        clusters = new CmdClusters(admin);

        clusters.run(
                split("create my-cluster --url http://my-service.url:8080 --url-secure https://my-service.url:4443"));
        verify(mockClusters).createCluster("my-cluster",
                new ClusterData("http://my-service.url:8080", "https://my-service.url:4443"));

        clusters.run(
                split("update my-cluster --url http://my-service.url:8080 --url-secure https://my-service.url:4443"));
        verify(mockClusters).updateCluster("my-cluster",
                new ClusterData("http://my-service.url:8080", "https://my-service.url:4443"));

        clusters.run(split("delete my-cluster"));
        verify(mockClusters).deleteCluster("my-cluster");
    }

    @Test
    void properties() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Properties mockProperties = mock(Properties.class);
        when(admin.properties()).thenReturn(mockProperties);

        CmdProperties properties = new CmdProperties(admin);

        properties.run(split("list"));
        verify(mockProperties).getProperties();

        PropertyAdmin propertyAdmin = new PropertyAdmin(Lists.newArrayList("role1", "role2"), Sets.newHashSet("use"));

        properties.run(split("create property --admin-roles role1,role2 --allowed-clusters use"));
        verify(mockProperties).createProperty("property", propertyAdmin);

        propertyAdmin = new PropertyAdmin(Lists.newArrayList("role1", "role2"), Sets.newHashSet("usw"));

        properties.run(split("update property --admin-roles role1,role2 --allowed-clusters usw"));
        verify(mockProperties).updateProperty("property", propertyAdmin);

        properties.run(split("get property"));
        verify(mockProperties).getPropertyAdmin("property");

        properties.run(split("delete property"));
        verify(mockProperties).deleteProperty("property");
    }

    @Test
    void namespaces() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Namespaces mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        Lookup mockLookup = mock(Lookup.class);
        when(admin.lookups()).thenReturn(mockLookup);

        CmdNamespaces namespaces = new CmdNamespaces(admin);

        namespaces.run(split("list myprop"));
        verify(mockNamespaces).getNamespaces("myprop");

        namespaces.run(split("list-cluster myprop/clust"));
        verify(mockNamespaces).getNamespaces("myprop", "clust");

        namespaces.run(split("destinations myprop/clust/ns1"));
        verify(mockNamespaces).getDestinations("myprop/clust/ns1");

        namespaces.run(split("policies myprop/clust/ns1"));
        verify(mockNamespaces).getPolicies("myprop/clust/ns1");

        namespaces.run(split("create myprop/clust/ns1"));
        verify(mockNamespaces).createNamespace("myprop/clust/ns1");

        namespaces.run(split("delete myprop/clust/ns1"));
        verify(mockNamespaces).deleteNamespace("myprop/clust/ns1");

        namespaces.run(split("permissions myprop/clust/ns1"));
        verify(mockNamespaces).getPermissions("myprop/clust/ns1");

        namespaces.run(split("grant-permission myprop/clust/ns1 --role role1 --actions produce,consume"));
        verify(mockNamespaces).grantPermissionOnNamespace("myprop/clust/ns1", "role1",
                EnumSet.of(AuthAction.produce, AuthAction.consume));

        namespaces.run(split("revoke-permission myprop/clust/ns1 --role role1"));
        verify(mockNamespaces).revokePermissionsOnNamespace("myprop/clust/ns1", "role1");

        namespaces.run(split("set-clusters myprop/clust/ns1 -c use,usw,usc"));
        verify(mockNamespaces).setNamespaceReplicationClusters("myprop/clust/ns1",
                Lists.newArrayList("use", "usw", "usc"));

        namespaces.run(split("get-clusters myprop/clust/ns1"));
        verify(mockNamespaces).getNamespaceReplicationClusters("myprop/clust/ns1");

        namespaces.run(split("unload myprop/clust/ns1"));
        verify(mockNamespaces).unload("myprop/clust/ns1");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(admin);

        namespaces.run(split("unload myprop/clust/ns1 -b 0x80000000_0xffffffff"));
        verify(mockNamespaces).unloadNamespaceBundle("myprop/clust/ns1", "0x80000000_0xffffffff");

        namespaces.run(split("split-bundle myprop/clust/ns1 -b 0x00000000_0xffffffff"));
        verify(mockNamespaces).splitNamespaceBundle("myprop/clust/ns1", "0x00000000_0xffffffff");

        namespaces.run(split("get-backlog-quotas myprop/clust/ns1"));
        verify(mockNamespaces).getBacklogQuotaMap("myprop/clust/ns1");

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_request_hold -l 10"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                new BacklogQuota(10, RetentionPolicy.producer_request_hold));

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(admin);

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_exception -l 10K"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                new BacklogQuota(10 * 1024, RetentionPolicy.producer_exception));

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(admin);

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_exception -l 10M"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                new BacklogQuota(10 * 1024 * 1024, RetentionPolicy.producer_exception));

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(admin);

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_exception -l 10G"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                new BacklogQuota(10l * 1024 * 1024 * 1024, RetentionPolicy.producer_exception));

        namespaces.run(split("set-persistence myprop/clust/ns1 -e 2 -w 1 -a 1 -r 100.0"));
        verify(mockNamespaces).setPersistence("myprop/clust/ns1", new PersistencePolicies(2, 1, 1, 100.0d));

        namespaces.run(split("get-persistence myprop/clust/ns1"));
        verify(mockNamespaces).getPersistence("myprop/clust/ns1");

        namespaces.run(split("set-message-ttl myprop/clust/ns1 -ttl 300"));
        verify(mockNamespaces).setNamespaceMessageTTL("myprop/clust/ns1", 300);

        namespaces.run(split("get-message-ttl myprop/clust/ns1"));
        verify(mockNamespaces).getNamespaceMessageTTL("myprop/clust/ns1");

        namespaces.run(split("set-retention myprop/clust/ns1 -t 1h -s 1M"));
        verify(mockNamespaces).setRetention("myprop/clust/ns1", new RetentionPolicies(60, 1));

        namespaces.run(split("get-retention myprop/clust/ns1"));
        verify(mockNamespaces).getRetention("myprop/clust/ns1");

        namespaces.run(split("clear-backlog myprop/clust/ns1 -force"));
        verify(mockNamespaces).clearNamespaceBacklog("myprop/clust/ns1");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(admin);

        namespaces.run(split("clear-backlog -b 0x80000000_0xffffffff myprop/clust/ns1 -force"));
        verify(mockNamespaces).clearNamespaceBundleBacklog("myprop/clust/ns1", "0x80000000_0xffffffff");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(admin);

        namespaces.run(split("clear-backlog -s my-sub myprop/clust/ns1 -force"));
        verify(mockNamespaces).clearNamespaceBacklogForSubscription("myprop/clust/ns1", "my-sub");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(admin);

        namespaces.run(split("clear-backlog -b 0x80000000_0xffffffff -s my-sub myprop/clust/ns1 -force"));
        verify(mockNamespaces).clearNamespaceBundleBacklogForSubscription("myprop/clust/ns1", "0x80000000_0xffffffff",
                "my-sub");

        namespaces.run(split("unsubscribe -s my-sub myprop/clust/ns1"));
        verify(mockNamespaces).unsubscribeNamespace("myprop/clust/ns1", "my-sub");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(admin);

        namespaces.run(split("unsubscribe -b 0x80000000_0xffffffff -s my-sub myprop/clust/ns1"));
        verify(mockNamespaces).unsubscribeNamespaceBundle("myprop/clust/ns1", "0x80000000_0xffffffff", "my-sub");
    }

    @Test
    void resourceQuotas() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        ResourceQuotas mockResourceQuotas = mock(ResourceQuotas.class);
        when(admin.resourceQuotas()).thenReturn(mockResourceQuotas);
        CmdResourceQuotas cmdResourceQuotas = new CmdResourceQuotas(admin);

        ResourceQuota quota = new ResourceQuota();
        quota.setMsgRateIn(10);
        quota.setMsgRateOut(20);
        quota.setBandwidthIn(10000);
        quota.setBandwidthOut(20000);
        quota.setMemory(100);
        quota.setDynamic(false);

        cmdResourceQuotas.run(split("get"));
        verify(mockResourceQuotas).getDefaultResourceQuota();

        cmdResourceQuotas.run(split("set -mi 10 -mo 20 -bi 10000 -bo 20000 -mem 100"));
        verify(mockResourceQuotas).setDefaultResourceQuota(quota);

        // reset mocks
        mockResourceQuotas = mock(ResourceQuotas.class);
        when(admin.resourceQuotas()).thenReturn(mockResourceQuotas);
        cmdResourceQuotas = new CmdResourceQuotas(admin);

        cmdResourceQuotas.run(split("get --namespace myprop/clust/ns1 --bundle 0x80000000_0xffffffff"));
        verify(mockResourceQuotas).getNamespaceBundleResourceQuota("myprop/clust/ns1", "0x80000000_0xffffffff");

        cmdResourceQuotas.run(split(
                "set --namespace myprop/clust/ns1 --bundle 0x80000000_0xffffffff -mi 10 -mo 20 -bi 10000 -bo 20000 -mem 100"));
        verify(mockResourceQuotas).setNamespaceBundleResourceQuota("myprop/clust/ns1", "0x80000000_0xffffffff", quota);

        cmdResourceQuotas
                .run(split("reset-namespace-bundle-quota --namespace myprop/clust/ns1 --bundle 0x80000000_0xffffffff"));
        verify(mockResourceQuotas).resetNamespaceBundleResourceQuota("myprop/clust/ns1", "0x80000000_0xffffffff");
    }

    @Test
    void persistentTopics() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        PersistentTopics mockTopics = mock(PersistentTopics.class);
        when(admin.persistentTopics()).thenReturn(mockTopics);

        CmdPersistentTopics topics = new CmdPersistentTopics(admin);

        topics.run(split("delete persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).delete("persistent://myprop/clust/ns1/ds1");

        topics.run(split("list myprop/clust/ns1"));
        verify(mockTopics).getList("myprop/clust/ns1");

        topics.run(split("subscriptions persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getSubscriptions("persistent://myprop/clust/ns1/ds1");

        topics.run(split("unsubscribe persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).deleteSubscription("persistent://myprop/clust/ns1/ds1", "sub1");

        topics.run(split("stats persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getStats("persistent://myprop/clust/ns1/ds1");

        topics.run(split("stats-internal persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getInternalStats("persistent://myprop/clust/ns1/ds1");

        topics.run(split("partitioned-stats persistent://myprop/clust/ns1/ds1 --per-partition"));
        verify(mockTopics).getPartitionedStats("persistent://myprop/clust/ns1/ds1", true);

        topics.run(split("skip-all persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).skipAllMessages("persistent://myprop/clust/ns1/ds1", "sub1");

        topics.run(split("skip persistent://myprop/clust/ns1/ds1 -s sub1 -n 100"));
        verify(mockTopics).skipMessages("persistent://myprop/clust/ns1/ds1", "sub1", 100);
        
        topics.run(split("expire-messages persistent://myprop/clust/ns1/ds1 -s sub1 -t 100"));
        verify(mockTopics).expireMessages("persistent://myprop/clust/ns1/ds1", "sub1", 100);
        
        topics.run(split("expire-messages-all-subscriptions persistent://myprop/clust/ns1/ds1 -t 100"));
        verify(mockTopics).expireMessagesForAllSubscriptions("persistent://myprop/clust/ns1/ds1", 100);

        topics.run(split("create-partitioned-topic persistent://myprop/clust/ns1/ds1 --partitions 32"));
        verify(mockTopics).createPartitionedTopic("persistent://myprop/clust/ns1/ds1", 32);

        topics.run(split("get-partitioned-topic-metadata persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getPartitionedTopicMetadata("persistent://myprop/clust/ns1/ds1");

        topics.run(split("delete-partitioned-topic persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).deletePartitionedTopic("persistent://myprop/clust/ns1/ds1");

        topics.run(split("peek-messages persistent://myprop/clust/ns1/ds1 -s sub1 -n 3"));
        verify(mockTopics).peekMessages("persistent://myprop/clust/ns1/ds1", "sub1", 3);

        // argument matcher for the timestamp in reset cursor. Since we can't verify exact timestamp, we check for a
        // range of +/- 1 second of the expected timestamp
        class TimestampMatcher extends ArgumentMatcher<Long> {
            @Override
            public boolean matches(Object argument) {
                long timestamp = (Long) argument;
                long expectedTimestamp = System.currentTimeMillis() - (1 * 60 * 1000);
                if (timestamp < (expectedTimestamp + 1000) && timestamp > (expectedTimestamp - 1000)) {
                    return true;
                }
                return false;
            }
        }
        topics.run(split("reset-cursor persistent://myprop/clust/ns1/ds1 -s sub1 -t 1m"));
        verify(mockTopics).resetCursor(Matchers.eq("persistent://myprop/clust/ns1/ds1"), Matchers.eq("sub1"),
                Matchers.longThat(new TimestampMatcher()));
    }

    @Test
    void tool() throws Exception {
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty("serviceUrl", "http://api.messaging.use.example.com:8080");
        PulsarAdminTool tool = new PulsarAdminTool(properties);

        assertEquals(tool.run(new String[0]), false);
        assertEquals(tool.run(split("clusters list")), false);
        assertEquals(tool.run(split("invalid")), false);
        assertEquals(tool.run(split("--help")), false);
    }

    String[] split(String s) {
        return s.split(" ");
    }
}
