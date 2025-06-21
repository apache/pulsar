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
package org.apache.pulsar.admin.cli;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertNotNull;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.admin.cli.extensions.CustomCommandFactory;
import org.apache.pulsar.admin.cli.utils.SchemaExtractor;
import org.apache.pulsar.client.admin.Bookies;
import org.apache.pulsar.client.admin.BrokerStats;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.ListNamespaceTopicsOptions;
import org.apache.pulsar.client.admin.ListTopicsOptions;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.NonPersistentTopics;
import org.apache.pulsar.client.admin.ProxyStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.ResourceQuotas;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.Transactions;
import org.apache.pulsar.client.admin.internal.OffloadProcessStatusImpl;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TransactionIsolationLevel;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesClusterInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.FailureDomain;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.LedgerInfo;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.ResourceQuota;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import picocli.CommandLine;

@Slf4j
public class PulsarAdminToolTest {

    @Test
    public void brokers() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Brokers mockBrokers = mock(Brokers.class);
        doReturn(mockBrokers).when(admin).brokers();

        CmdBrokers brokers = new CmdBrokers(() -> admin);

        brokers.run(split("list use"));
        verify(mockBrokers).getActiveBrokers("use");

        brokers.run(split("leader-broker"));
        verify(mockBrokers).getLeaderBroker();

        brokers.run(split("namespaces use --url http://my-service.url:8080"));
        verify(mockBrokers).getOwnedNamespaces("use", "http://my-service.url:8080");

        brokers.run(split("get-all-dynamic-config"));
        verify(mockBrokers).getAllDynamicConfigurations();

        brokers.run(split("list-dynamic-config"));
        verify(mockBrokers).getDynamicConfigurationNames();

        brokers.run(split("update-dynamic-config --config brokerShutdownTimeoutMs --value 100"));
        verify(mockBrokers).updateDynamicConfiguration("brokerShutdownTimeoutMs", "100");

        brokers.run(split("delete-dynamic-config --config brokerShutdownTimeoutMs"));
        verify(mockBrokers).deleteDynamicConfiguration("brokerShutdownTimeoutMs");

        brokers.run(split("get-internal-config"));
        verify(mockBrokers).getInternalConfigurationData();

        brokers.run(split("get-runtime-config"));
        verify(mockBrokers).getRuntimeConfigurations();

        brokers.run(split("healthcheck"));
        verify(mockBrokers).healthcheck(null);

        brokers.run(split("version"));
        verify(mockBrokers).getVersion();

        doReturn(CompletableFuture.completedFuture(null)).when(mockBrokers)
                .shutDownBrokerGracefully(anyInt(), anyBoolean());
        brokers.run(split("shutdown -m 10 -f"));
        verify(mockBrokers).shutDownBrokerGracefully(10,true);
    }

    @Test
    public void brokerStats() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        BrokerStats mockBrokerStats = mock(BrokerStats.class);
        doReturn(mockBrokerStats).when(admin).brokerStats();

        CmdBrokerStats brokerStats = new CmdBrokerStats(() -> admin);

        doReturn("null").when(mockBrokerStats).getTopics();
        brokerStats.run(split("topics"));
        verify(mockBrokerStats).getTopics();

        doReturn(null).when(mockBrokerStats).getLoadReport();
        brokerStats.run(split("load-report"));
        verify(mockBrokerStats).getLoadReport();

        doReturn("null").when(mockBrokerStats).getMBeans();
        brokerStats.run(split("mbeans"));
        verify(mockBrokerStats).getMBeans();

        doReturn("null").when(mockBrokerStats).getMetrics();
        brokerStats.run(split("monitoring-metrics"));
        verify(mockBrokerStats).getMetrics();

        doReturn(null).when(mockBrokerStats).getAllocatorStats("default");
        brokerStats.run(split("allocator-stats default"));
        verify(mockBrokerStats).getAllocatorStats("default");
    }

    @Test
    public void getOwnedNamespaces() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Brokers mockBrokers = mock(Brokers.class);
        doReturn(mockBrokers).when(admin).brokers();

        CmdBrokers brokers = new CmdBrokers(() -> admin);

        brokers.run(split("namespaces use --url http://my-service.url:4000"));
        verify(mockBrokers).getOwnedNamespaces("use", "http://my-service.url:4000");

    }

    @Test
    public void clusters() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Clusters mockClusters = mock(Clusters.class);
        when(admin.clusters()).thenReturn(mockClusters);

        CmdClusters clusters = new CmdClusters(() -> admin);

        clusters.run(split("list"));
        verify(mockClusters).getClusters();

        clusters.run(split("get use"));
        verify(mockClusters).getCluster("use");

        clusters.run(split("create use --url http://my-service.url:8080"));
        verify(mockClusters).createCluster("use", ClusterData.builder().serviceUrl("http://my-service.url:8080").build());

        clusters.run(split("update use --url http://my-service.url:8080"));
        verify(mockClusters).updateCluster("use", ClusterData.builder().serviceUrl("http://my-service.url:8080").build());

        clusters.run(split("delete use"));
        verify(mockClusters).deleteCluster("use");

        clusters.run(split("list-failure-domains use"));
        verify(mockClusters).getFailureDomains("use");

        clusters.run(split("get-failure-domain use --domain-name domain"));
        verify(mockClusters).getFailureDomain("use", "domain");

        clusters.run(split("create-failure-domain use --domain-name domain --broker-list b1"));
        FailureDomain domain = FailureDomain.builder()
                .brokers(Collections.singleton("b1"))
                .build();
        verify(mockClusters).createFailureDomain("use", "domain", domain);

        clusters.run(split("update-failure-domain use --domain-name domain --broker-list b1"));
        verify(mockClusters).updateFailureDomain("use", "domain", domain);

        clusters.run(split("delete-failure-domain use --domain-name domain"));
        verify(mockClusters).deleteFailureDomain("use", "domain");


        // Re-create CmdClusters to avoid a issue.
        // See https://github.com/cbeust/jcommander/issues/271
        clusters = new CmdClusters(() -> admin);

        clusters.run(
                split("create my-cluster --url http://my-service.url:8080 --url-secure https://my-service.url:4443"));
        verify(mockClusters).createCluster("my-cluster",
                ClusterData.builder()
                        .serviceUrl("http://my-service.url:8080")
                        .serviceUrlTls("https://my-service.url:4443")
                        .build());

        clusters.run(
                split("update my-cluster --url http://my-service.url:8080 --url-secure https://my-service.url:4443"));
        verify(mockClusters).updateCluster("my-cluster",
                ClusterData.builder()
                        .serviceUrl("http://my-service.url:8080")
                        .serviceUrlTls("https://my-service.url:4443")
                        .build());

        clusters.run(split("delete my-cluster"));
        verify(mockClusters).deleteCluster("my-cluster");

        clusters.run(split("update-peer-clusters my-cluster --peer-clusters c1,c2"));
        verify(mockClusters).updatePeerClusterNames("my-cluster",
                Sets.newLinkedHashSet(Lists.newArrayList("c1", "c2")));

        clusters.run(split("get-peer-clusters my-cluster"));
        verify(mockClusters).getPeerClusterNames("my-cluster");

        // test create cluster without --url
        clusters = new CmdClusters(() -> admin);

        clusters.run(split("create my-secure-cluster --url-secure https://my-service.url:4443"));
        verify(mockClusters).createCluster("my-secure-cluster",
                ClusterData.builder()
                        .serviceUrlTls("https://my-service.url:4443")
                        .build());

        clusters.run(split("update my-secure-cluster --url-secure https://my-service.url:4443"));
        verify(mockClusters).updateCluster("my-secure-cluster",
                ClusterData.builder()
                        .serviceUrlTls("https://my-service.url:4443")
                        .build());

        clusters.run(split("delete my-secure-cluster"));
        verify(mockClusters).deleteCluster("my-secure-cluster");

        // test create cluster with tls
        clusters = new CmdClusters(() -> admin);
        clusters.run(split("create my-tls-cluster --url-secure https://my-service.url:4443 --tls-enable "
                + "--tls-enable-keystore --tls-trust-store-type JKS --tls-trust-store /var/private/tls/client.truststore.jks "
                + "--tls-trust-store-pwd clientpw --tls-key-store-type KEYSTORE_TYPE --tls-key-store /var/private/tls/client.keystore.jks "
                + "--tls-key-store-pwd KEYSTORE_STORE_PWD"));
        ClusterData.Builder data = ClusterData.builder()
                .serviceUrlTls("https://my-service.url:4443")
                .brokerClientTlsEnabled(true)
                .brokerClientTlsEnabledWithKeyStore(true)
                .brokerClientTlsTrustStoreType("JKS")
                .brokerClientTlsTrustStore("/var/private/tls/client.truststore.jks")
                .brokerClientTlsTrustStorePassword("clientpw")
                .brokerClientTlsKeyStoreType("KEYSTORE_TYPE")
                .brokerClientTlsKeyStore("/var/private/tls/client.keystore.jks")
                .brokerClientTlsKeyStorePassword("KEYSTORE_STORE_PWD");

        verify(mockClusters).createCluster("my-tls-cluster", data.build());

        clusters.run(split("update my-tls-cluster --url-secure https://my-service.url:4443 --tls-enable "
                + "--tls-trust-certs-filepath /path/to/ca.cert.pem --tls-key-filepath KEY_FILEPATH --tls-certs-filepath CERTS_FILEPATH"));
        data.brokerClientTlsEnabledWithKeyStore(false)
                .brokerClientTlsTrustStore(null)
                .brokerClientTlsTrustStorePassword(null)
                .brokerClientTlsKeyStoreType("JKS")
                .brokerClientTlsKeyStore(null)
                .brokerClientTlsKeyStorePassword(null)
                .brokerClientTrustCertsFilePath("/path/to/ca.cert.pem")
                .brokerClientKeyFilePath("KEY_FILEPATH")
                .brokerClientCertificateFilePath("CERTS_FILEPATH");
        verify(mockClusters).updateCluster("my-tls-cluster", data.build());
    }

    @Test
    public void tenants() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Tenants mockTenants = mock(Tenants.class);
        when(admin.tenants()).thenReturn(mockTenants);

        CmdTenants tenants = new CmdTenants(() -> admin);

        tenants.run(split("list"));
        verify(mockTenants).getTenants();

        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("use"));

        tenants.run(split("create my-tenant --admin-roles role1,role2 --allowed-clusters use"));
        verify(mockTenants).createTenant("my-tenant", tenantInfo);

        tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("usw"));

        tenants.run(split("update my-tenant --admin-roles role1,role2 --allowed-clusters usw"));
        verify(mockTenants).updateTenant("my-tenant", tenantInfo);

        tenants.run(split("get my-tenant"));
        verify(mockTenants).getTenantInfo("my-tenant");

        tenants.run(split("delete my-tenant"));
        verify(mockTenants).deleteTenant("my-tenant", false);
    }

    @Test
    public void namespacesSetOffloadPolicies() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Namespaces mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        Lookup mockLookup = mock(Lookup.class);
        when(admin.lookups()).thenReturn(mockLookup);

        // filesystem offload
        CmdNamespaces namespaces = new CmdNamespaces(() -> admin);
        namespaces.run(split(
          "set-offload-policies myprop/clust/ns2 -d filesystem -oat 100M -oats 1h -oae 1h -orp bookkeeper-first"));
        verify(mockNamespaces).setOffloadPolicies("myprop/clust/ns2",
          OffloadPoliciesImpl.create("filesystem", null, null,
            null, null, null, null, null, 64 * 1024 * 1024, 1024 * 1024,
            100 * 1024 * 1024L, 3600L, 3600 * 1000L, OffloadedReadPriority.BOOKKEEPER_FIRST));

        // S3 offload
        CmdNamespaces namespaces2 = new CmdNamespaces(() -> admin);
        namespaces2.run(split(
          "set-offload-policies myprop/clust/ns1 -r test-region -d aws-s3 -b test-bucket -e http://test.endpoint -mbs 32M -rbs 5M -oat 10M -oats 100 -oae 10s -orp tiered-storage-first"));
        verify(mockNamespaces).setOffloadPolicies("myprop/clust/ns1",
          OffloadPoliciesImpl.create("aws-s3", "test-region", "test-bucket",
            "http://test.endpoint",null, null, null, null, 32 * 1024 * 1024, 5 * 1024 * 1024,
            10 * 1024 * 1024L, 100L, 10000L, OffloadedReadPriority.TIERED_STORAGE_FIRST));
    }

        @Test
    public void namespaces() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Namespaces mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        Lookup mockLookup = mock(Lookup.class);
        when(admin.lookups()).thenReturn(mockLookup);

        CmdNamespaces namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("list myprop"));
        verify(mockNamespaces).getNamespaces("myprop");

        namespaces.run(split("list-cluster myprop/clust"));
        verify(mockNamespaces).getNamespaces("myprop", "clust");

        namespaces.run(split("topics myprop/clust/ns1"));
        verify(mockNamespaces).getTopics("myprop/clust/ns1", ListNamespaceTopicsOptions.builder().build());

        namespaces.run(split("policies myprop/clust/ns1"));
        verify(mockNamespaces).getPolicies("myprop/clust/ns1");

        namespaces.run(split("create myprop/clust/ns1"));
        verify(mockNamespaces).createNamespace("myprop/clust/ns1");

        namespaces.run(split("delete myprop/clust/ns1"));
        verify(mockNamespaces).deleteNamespace("myprop/clust/ns1", false);

        namespaces.run(split("permissions myprop/clust/ns1"));
        verify(mockNamespaces).getPermissions("myprop/clust/ns1");

        namespaces.run(split("grant-permission myprop/clust/ns1 --role role1 --actions produce,consume"));
        verify(mockNamespaces).grantPermissionOnNamespace("myprop/clust/ns1", "role1",
                EnumSet.of(AuthAction.produce, AuthAction.consume));

        namespaces.run(split("revoke-permission myprop/clust/ns1 --role role1"));
        verify(mockNamespaces).revokePermissionsOnNamespace("myprop/clust/ns1", "role1");

        namespaces.run(split("set-clusters myprop/clust/ns1 -c use,usw,usc"));
        verify(mockNamespaces).setNamespaceReplicationClusters("myprop/clust/ns1",
                Sets.newHashSet("use", "usw", "usc"));

        namespaces.run(split("get-clusters myprop/clust/ns1"));
        verify(mockNamespaces).getNamespaceReplicationClusters("myprop/clust/ns1");

            namespaces.run(split("set-allowed-clusters myprop/clust/ns1 -c use,usw,usc"));
            verify(mockNamespaces).setNamespaceAllowedClusters("myprop/clust/ns1",
                    Sets.newHashSet("use", "usw", "usc"));

            namespaces.run(split("get-allowed-clusters myprop/clust/ns1"));
            verify(mockNamespaces).getNamespaceAllowedClusters("myprop/clust/ns1");


            namespaces.run(split("set-subscription-types-enabled myprop/clust/ns1 -t Shared,Failover"));
        verify(mockNamespaces).setSubscriptionTypesEnabled("myprop/clust/ns1",
                Sets.newHashSet(SubscriptionType.Shared, SubscriptionType.Failover));

        namespaces.run(split("get-subscription-types-enabled myprop/clust/ns1"));
        verify(mockNamespaces).getSubscriptionTypesEnabled("myprop/clust/ns1");

        namespaces.run(split("remove-subscription-types-enabled myprop/clust/ns1"));
        verify(mockNamespaces).removeSubscriptionTypesEnabled("myprop/clust/ns1");

        namespaces.run(split("get-schema-validation-enforce myprop/clust/ns1 -ap"));
        verify(mockNamespaces).getSchemaValidationEnforced("myprop/clust/ns1", true);

        namespaces
                .run(split("set-bookie-affinity-group myprop/clust/ns1 --primary-group test1 --secondary-group test2"));
        verify(mockNamespaces).setBookieAffinityGroup("myprop/clust/ns1",
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary("test1")
                        .bookkeeperAffinityGroupSecondary("test2")
                        .build());

        namespaces.run(split("get-bookie-affinity-group myprop/clust/ns1"));
        verify(mockNamespaces).getBookieAffinityGroup("myprop/clust/ns1");

        namespaces.run(split("delete-bookie-affinity-group myprop/clust/ns1"));
        verify(mockNamespaces).deleteBookieAffinityGroup("myprop/clust/ns1");

        namespaces.run(split("set-replicator-dispatch-rate myprop/clust/ns1 -md 10 -bd 11 -dt 12"));
        verify(mockNamespaces).setReplicatorDispatchRate("myprop/clust/ns1", DispatchRate.builder()
                .dispatchThrottlingRateInMsg(10)
                .dispatchThrottlingRateInByte(11)
                .ratePeriodInSecond(12)
                .build());

        namespaces.run(split("get-replicator-dispatch-rate myprop/clust/ns1"));
        verify(mockNamespaces).getReplicatorDispatchRate("myprop/clust/ns1");

        namespaces.run(split("remove-replicator-dispatch-rate myprop/clust/ns1"));
        verify(mockNamespaces).removeReplicatorDispatchRate("myprop/clust/ns1");


        assertFalse(namespaces.run(split("unload myprop/clust/ns1 -d broker")));
        verify(mockNamespaces, times(0)).unload("myprop/clust/ns1");

        namespaces = new CmdNamespaces(() -> admin);
        namespaces.run(split("unload myprop/clust/ns1"));
        verify(mockNamespaces).unload("myprop/clust/ns1");

        // message_age must have time limit, destination_storage must have size limit
        Assert.assertFalse(namespaces.run(
                split("set-backlog-quota myprop/clust/ns1 -p producer_exception -l 10G -t message_age")));
        Assert.assertFalse(namespaces.run(
                split("set-backlog-quota myprop/clust/ns1 -p producer_exception -lt 10h -t destination_storage")));

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("unload myprop/clust/ns1 -b 0x80000000_0xffffffff"));
        verify(mockNamespaces).unloadNamespaceBundle("myprop/clust/ns1", "0x80000000_0xffffffff", null);

        namespaces = new CmdNamespaces(() -> admin);
        namespaces.run(split("unload myprop/clust/ns1 -b 0x80000000_0xffffffff -d broker"));
        verify(mockNamespaces).unloadNamespaceBundle("myprop/clust/ns1", "0x80000000_0xffffffff", "broker");

        namespaces.run(split("split-bundle myprop/clust/ns1 -b 0x00000000_0xffffffff"));
        verify(mockNamespaces).splitNamespaceBundle("myprop/clust/ns1", "0x00000000_0xffffffff", false, null);

        namespaces.run(split("get-backlog-quotas myprop/clust/ns1"));
        verify(mockNamespaces).getBacklogQuotaMap("myprop/clust/ns1");

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_request_hold -l 10"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                BacklogQuota.builder()
                        .limitSize(10)
                        .retentionPolicy(RetentionPolicy.producer_request_hold)
                        .build(),
                        BacklogQuota.BacklogQuotaType.destination_storage);

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_exception -l 10K"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                BacklogQuota.builder()
                        .limitSize(10 * 1024)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build(),
                        BacklogQuota.BacklogQuotaType.destination_storage);

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_exception -l 10M"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                BacklogQuota.builder()
                        .limitSize(10 * 1024 * 1024)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build(),
                        BacklogQuota.BacklogQuotaType.destination_storage);

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_exception -l 10G"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                BacklogQuota.builder()
                        .limitSize(10L * 1024 * 1024 * 1024)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build(),
                        BacklogQuota.BacklogQuotaType.destination_storage);

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p consumer_backlog_eviction -lt 10m -t message_age"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                BacklogQuota.builder()
                        .limitTime(10 * 60)
                        .retentionPolicy(RetentionPolicy.consumer_backlog_eviction)
                        .build(),
                        BacklogQuota.BacklogQuotaType.message_age);

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("set-backlog-quota myprop/clust/ns1 -p producer_exception -lt 10000 -t message_age"));
        verify(mockNamespaces).setBacklogQuota("myprop/clust/ns1",
                BacklogQuota.builder()
                        .limitTime(10000)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build(),
                        BacklogQuota.BacklogQuotaType.message_age);

        namespaces.run(split("set-persistence myprop/clust/ns1 -e 2 -w 1 -a 1 -r 100.0"));
        verify(mockNamespaces).setPersistence("myprop/clust/ns1",
                new PersistencePolicies(2, 1, 1, 100.0d));

        namespaces.run(split("get-persistence myprop/clust/ns1"));
        verify(mockNamespaces).getPersistence("myprop/clust/ns1");

        namespaces.run(split("remove-persistence myprop/clust/ns1"));
        verify(mockNamespaces).removePersistence("myprop/clust/ns1");

        namespaces.run(split("get-max-subscriptions-per-topic myprop/clust/ns1"));
        verify(mockNamespaces).getMaxSubscriptionsPerTopic("myprop/clust/ns1");
        namespaces.run(split("set-max-subscriptions-per-topic myprop/clust/ns1 -m 300"));
        verify(mockNamespaces).setMaxSubscriptionsPerTopic("myprop/clust/ns1", 300);
        namespaces.run(split("remove-max-subscriptions-per-topic myprop/clust/ns1"));
        verify(mockNamespaces).removeMaxSubscriptionsPerTopic("myprop/clust/ns1");

        namespaces.run(split("set-message-ttl myprop/clust/ns1 -ttl 300"));
        verify(mockNamespaces).setNamespaceMessageTTL("myprop/clust/ns1", 300);

        namespaces.run(split("set-subscription-expiration-time myprop/clust/ns1 -t 60"));
        verify(mockNamespaces).setSubscriptionExpirationTime("myprop/clust/ns1", 60);

        namespaces.run(split("get-deduplication myprop/clust/ns1"));
        verify(mockNamespaces).getDeduplicationStatus("myprop/clust/ns1");
        namespaces.run(split("set-deduplication myprop/clust/ns1 --enable"));
        verify(mockNamespaces).setDeduplicationStatus("myprop/clust/ns1", true);
        namespaces.run(split("remove-deduplication myprop/clust/ns1"));
        verify(mockNamespaces).removeDeduplicationStatus("myprop/clust/ns1");

        namespaces.run(split("set-auto-topic-creation myprop/clust/ns1 -e -t non-partitioned"));
        verify(mockNamespaces).setAutoTopicCreation("myprop/clust/ns1",
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType(TopicType.NON_PARTITIONED.toString())
                        .build());

        namespaces.run(split("get-auto-topic-creation myprop/clust/ns1"));
        verify(mockNamespaces).getAutoTopicCreation("myprop/clust/ns1");

        namespaces.run(split("remove-auto-topic-creation myprop/clust/ns1"));
        verify(mockNamespaces).removeAutoTopicCreation("myprop/clust/ns1");

        namespaces.run(split("set-auto-subscription-creation myprop/clust/ns1 -e"));
        verify(mockNamespaces).setAutoSubscriptionCreation("myprop/clust/ns1",
                AutoSubscriptionCreationOverride.builder().allowAutoSubscriptionCreation(true).build());

        namespaces.run(split("get-auto-subscription-creation myprop/clust/ns1"));
        verify(mockNamespaces).getAutoSubscriptionCreation("myprop/clust/ns1");

        namespaces.run(split("remove-auto-subscription-creation myprop/clust/ns1"));
        verify(mockNamespaces).removeAutoSubscriptionCreation("myprop/clust/ns1");

        namespaces.run(split("get-message-ttl myprop/clust/ns1"));
        verify(mockNamespaces).getNamespaceMessageTTL("myprop/clust/ns1");

        namespaces.run(split("get-subscription-expiration-time myprop/clust/ns1"));
        verify(mockNamespaces).getSubscriptionExpirationTime("myprop/clust/ns1");

        namespaces.run(split("remove-subscription-expiration-time myprop/clust/ns1"));
        verify(mockNamespaces).removeSubscriptionExpirationTime("myprop/clust/ns1");

        namespaces.run(split("set-anti-affinity-group myprop/clust/ns1 -g group"));
        verify(mockNamespaces).setNamespaceAntiAffinityGroup("myprop/clust/ns1", "group");

        namespaces.run(split("get-anti-affinity-group myprop/clust/ns1"));
        verify(mockNamespaces).getNamespaceAntiAffinityGroup("myprop/clust/ns1");

        namespaces.run(split("get-anti-affinity-namespaces -p dummy -c cluster -g group"));
        verify(mockNamespaces).getAntiAffinityNamespaces("dummy", "cluster", "group");

        namespaces.run(split("delete-anti-affinity-group myprop/clust/ns1 "));
        verify(mockNamespaces).deleteNamespaceAntiAffinityGroup("myprop/clust/ns1");


        namespaces.run(split("set-retention myprop/clust/ns1 -t 1h -s 1M"));
        verify(mockNamespaces).setRetention("myprop/clust/ns1",
                new RetentionPolicies(60, 1));

        // Test with default time unit (seconds)
        namespaces = new CmdNamespaces(() -> admin);
        reset(mockNamespaces);
        namespaces.run(split("set-retention myprop/clust/ns1 -t 120 -s 20M"));
        verify(mockNamespaces).setRetention("myprop/clust/ns1",
                new RetentionPolicies(2, 20));

        // Test with explicit time unit (seconds)
        namespaces = new CmdNamespaces(() -> admin);
        reset(mockNamespaces);
        namespaces.run(split("set-retention myprop/clust/ns1 -t 120s -s 20M"));
        verify(mockNamespaces).setRetention("myprop/clust/ns1",
                new RetentionPolicies(2, 20));

        // Test size with default size less than 1 mb
        namespaces = new CmdNamespaces(() -> admin);
        reset(mockNamespaces);
        namespaces.run(split("set-retention myprop/clust/ns1 -t 120s -s 4096"));
        verify(mockNamespaces).setRetention("myprop/clust/ns1",
                new RetentionPolicies(2, 0));

        // Test size with default size greater than 1mb
        namespaces = new CmdNamespaces(() -> admin);
        reset(mockNamespaces);
        namespaces.run(split("set-retention myprop/clust/ns1 -t 180 -s " + (2 * 1024 * 1024)));
        verify(mockNamespaces).setRetention("myprop/clust/ns1",
                new RetentionPolicies(3, 2));

        namespaces.run(split("get-retention myprop/clust/ns1"));
        verify(mockNamespaces).getRetention("myprop/clust/ns1");

        namespaces.run(split("remove-retention myprop/clust/ns1"));
        verify(mockNamespaces).removeRetention("myprop/clust/ns1");

        namespaces.run(split("set-delayed-delivery myprop/clust/ns1 -e -t 1s -md 5s"));
        verify(mockNamespaces).setDelayedDeliveryMessages("myprop/clust/ns1",
                DelayedDeliveryPolicies.builder().tickTime(1000).active(true).maxDeliveryDelayInMillis(5000).build());

        namespaces.run(split("get-delayed-delivery myprop/clust/ns1"));
        verify(mockNamespaces).getDelayedDelivery("myprop/clust/ns1");

        namespaces.run(split("remove-delayed-delivery myprop/clust/ns1"));
        verify(mockNamespaces).removeDelayedDeliveryMessages("myprop/clust/ns1");

        namespaces.run(split("set-inactive-topic-policies myprop/clust/ns1 -e -t 1s -m delete_when_no_subscriptions"));
        verify(mockNamespaces).setInactiveTopicPolicies("myprop/clust/ns1",
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1,
                        true));

        namespaces.run(split("get-inactive-topic-policies myprop/clust/ns1"));
        verify(mockNamespaces).getInactiveTopicPolicies("myprop/clust/ns1");

        namespaces.run(split("remove-inactive-topic-policies myprop/clust/ns1"));
        verify(mockNamespaces).removeInactiveTopicPolicies("myprop/clust/ns1");

        namespaces.run(split("clear-backlog myprop/clust/ns1 -force"));
        verify(mockNamespaces).clearNamespaceBacklog("myprop/clust/ns1");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("set-message-ttl myprop/clust/ns1 -ttl 6m"));
        verify(mockNamespaces).setNamespaceMessageTTL("myprop/clust/ns1", 6 * 60);

        namespaces.run(split("clear-backlog -b 0x80000000_0xffffffff myprop/clust/ns1 -force"));
        verify(mockNamespaces).clearNamespaceBundleBacklog("myprop/clust/ns1", "0x80000000_0xffffffff");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("clear-backlog -s my-sub myprop/clust/ns1 -force"));
        verify(mockNamespaces).clearNamespaceBacklogForSubscription("myprop/clust/ns1", "my-sub");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("clear-backlog -b 0x80000000_0xffffffff -s my-sub myprop/clust/ns1 -force"));
        verify(mockNamespaces).clearNamespaceBundleBacklogForSubscription("myprop/clust/ns1", "0x80000000_0xffffffff",
                "my-sub");

        namespaces.run(split("unsubscribe -s my-sub myprop/clust/ns1"));
        verify(mockNamespaces).unsubscribeNamespace("myprop/clust/ns1", "my-sub");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("unsubscribe -b 0x80000000_0xffffffff -s my-sub myprop/clust/ns1"));
        verify(mockNamespaces).unsubscribeNamespaceBundle("myprop/clust/ns1", "0x80000000_0xffffffff", "my-sub");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("get-max-producers-per-topic myprop/clust/ns1"));
        verify(mockNamespaces).getMaxProducersPerTopic("myprop/clust/ns1");

        namespaces.run(split("set-max-producers-per-topic myprop/clust/ns1 -p 1"));
        verify(mockNamespaces).setMaxProducersPerTopic("myprop/clust/ns1", 1);

        namespaces.run(split("remove-max-producers-per-topic myprop/clust/ns1"));
        verify(mockNamespaces).removeMaxProducersPerTopic("myprop/clust/ns1");

        namespaces.run(split("get-max-consumers-per-topic myprop/clust/ns1"));
        verify(mockNamespaces).getMaxConsumersPerTopic("myprop/clust/ns1");

        namespaces.run(split("set-max-consumers-per-topic myprop/clust/ns1 -c 2"));
        verify(mockNamespaces).setMaxConsumersPerTopic("myprop/clust/ns1", 2);

        namespaces.run(split("remove-max-consumers-per-topic myprop/clust/ns1"));
        verify(mockNamespaces).removeMaxConsumersPerTopic("myprop/clust/ns1");

        namespaces.run(split("get-max-consumers-per-subscription myprop/clust/ns1"));
        verify(mockNamespaces).getMaxConsumersPerSubscription("myprop/clust/ns1");

        namespaces.run(split("remove-max-consumers-per-subscription myprop/clust/ns1"));
        verify(mockNamespaces).removeMaxConsumersPerSubscription("myprop/clust/ns1");

        namespaces.run(split("set-max-consumers-per-subscription myprop/clust/ns1 -c 3"));
        verify(mockNamespaces).setMaxConsumersPerSubscription("myprop/clust/ns1", 3);

        namespaces.run(split("get-max-unacked-messages-per-subscription myprop/clust/ns1"));
        verify(mockNamespaces).getMaxUnackedMessagesPerSubscription("myprop/clust/ns1");

        namespaces.run(split("set-max-unacked-messages-per-subscription myprop/clust/ns1 -c 3"));
        verify(mockNamespaces).setMaxUnackedMessagesPerSubscription("myprop/clust/ns1", 3);

        namespaces.run(split("remove-max-unacked-messages-per-subscription myprop/clust/ns1"));
        verify(mockNamespaces).removeMaxUnackedMessagesPerSubscription("myprop/clust/ns1");

        namespaces.run(split("get-max-unacked-messages-per-consumer myprop/clust/ns1"));
        verify(mockNamespaces).getMaxUnackedMessagesPerConsumer("myprop/clust/ns1");

        namespaces.run(split("set-max-unacked-messages-per-consumer myprop/clust/ns1 -c 3"));
        verify(mockNamespaces).setMaxUnackedMessagesPerConsumer("myprop/clust/ns1", 3);

        namespaces.run(split("remove-max-unacked-messages-per-consumer myprop/clust/ns1"));
        verify(mockNamespaces).removeMaxUnackedMessagesPerConsumer("myprop/clust/ns1");

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("set-dispatch-rate myprop/clust/ns1 -md -1 -bd -1 -dt 2"));
        verify(mockNamespaces).setDispatchRate("myprop/clust/ns1", DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(2)
                .build());

        namespaces.run(split("get-dispatch-rate myprop/clust/ns1"));
        verify(mockNamespaces).getDispatchRate("myprop/clust/ns1");

        namespaces.run(split("remove-dispatch-rate myprop/clust/ns1"));
        verify(mockNamespaces).removeDispatchRate("myprop/clust/ns1");

        namespaces.run(split("set-publish-rate myprop/clust/ns1 -m 10 -b 20"));
        verify(mockNamespaces).setPublishRate("myprop/clust/ns1", new PublishRate(10, 20));

        namespaces.run(split("get-publish-rate myprop/clust/ns1"));
        verify(mockNamespaces).getPublishRate("myprop/clust/ns1");

        namespaces.run(split("remove-publish-rate myprop/clust/ns1"));
        verify(mockNamespaces).removePublishRate("myprop/clust/ns1");

        namespaces.run(split("set-subscribe-rate myprop/clust/ns1 -sr 2 -st 60"));
        verify(mockNamespaces).setSubscribeRate("myprop/clust/ns1", new SubscribeRate(2, 60));

        namespaces.run(split("get-subscribe-rate myprop/clust/ns1"));
        verify(mockNamespaces).getSubscribeRate("myprop/clust/ns1");

        namespaces.run(split("remove-subscribe-rate myprop/clust/ns1"));
        verify(mockNamespaces).removeSubscribeRate("myprop/clust/ns1");

        namespaces.run(split("set-subscription-dispatch-rate myprop/clust/ns1 -md -1 -bd -1 -dt 2"));
        verify(mockNamespaces).setSubscriptionDispatchRate("myprop/clust/ns1", DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(2)
                .build());

        namespaces.run(split("get-subscription-dispatch-rate myprop/clust/ns1"));
        verify(mockNamespaces).getSubscriptionDispatchRate("myprop/clust/ns1");

        namespaces.run(split("remove-subscription-dispatch-rate myprop/clust/ns1"));
        verify(mockNamespaces).removeSubscriptionDispatchRate("myprop/clust/ns1");

        namespaces.run(split("get-compaction-threshold myprop/clust/ns1"));
        verify(mockNamespaces).getCompactionThreshold("myprop/clust/ns1");

        namespaces.run(split("remove-compaction-threshold myprop/clust/ns1"));
        verify(mockNamespaces).removeCompactionThreshold("myprop/clust/ns1");

        namespaces.run(split("set-compaction-threshold myprop/clust/ns1 -t 1G"));
        verify(mockNamespaces).setCompactionThreshold("myprop/clust/ns1", 1024 * 1024 * 1024);

        namespaces.run(split("get-offload-threshold myprop/clust/ns1"));
        verify(mockNamespaces).getOffloadThreshold("myprop/clust/ns1");

        namespaces.run(split("set-offload-threshold myprop/clust/ns1 -s 1G"));
        verify(mockNamespaces).setOffloadThreshold("myprop/clust/ns1", 1024 * 1024 * 1024);

        namespaces.run(split("get-offload-deletion-lag myprop/clust/ns1"));
        verify(mockNamespaces).getOffloadDeleteLagMs("myprop/clust/ns1");

        namespaces.run(split("set-offload-deletion-lag myprop/clust/ns1 -l 1d"));
        verify(mockNamespaces).setOffloadDeleteLag("myprop/clust/ns1", 24 * 60 * 60, TimeUnit.SECONDS);

        namespaces.run(split("clear-offload-deletion-lag myprop/clust/ns1"));
        verify(mockNamespaces).clearOffloadDeleteLag("myprop/clust/ns1");

        namespaces.run(split(
                "set-offload-policies myprop/clust/ns1 -r test-region -d aws-s3 -b test-bucket -e http://test.endpoint -mbs 32M -rbs 5M -oat 10M -oats 100 -oae 10s -orp tiered-storage-first"));
        verify(mockNamespaces).setOffloadPolicies("myprop/clust/ns1",
                OffloadPoliciesImpl.create("aws-s3", "test-region", "test-bucket",
                        "http://test.endpoint",null, null, null, null, 32 * 1024 * 1024, 5 * 1024 * 1024,
                        10 * 1024 * 1024L, 100L, 10000L, OffloadedReadPriority.TIERED_STORAGE_FIRST));

        namespaces.run(split("remove-offload-policies myprop/clust/ns1"));
        verify(mockNamespaces).removeOffloadPolicies("myprop/clust/ns1");

        namespaces.run(split("get-offload-policies myprop/clust/ns1"));
        verify(mockNamespaces).getOffloadPolicies("myprop/clust/ns1");

        namespaces.run(split("remove-message-ttl myprop/clust/ns1"));
        verify(mockNamespaces).removeNamespaceMessageTTL("myprop/clust/ns1");

        namespaces.run(split("set-deduplication-snapshot-interval myprop/clust/ns1 -i 1000"));
        verify(mockNamespaces).setDeduplicationSnapshotInterval("myprop/clust/ns1", 1000);
        namespaces.run(split("get-deduplication-snapshot-interval myprop/clust/ns1"));
        verify(mockNamespaces).getDeduplicationSnapshotInterval("myprop/clust/ns1");
        namespaces.run(split("remove-deduplication-snapshot-interval myprop/clust/ns1"));
        verify(mockNamespaces).removeDeduplicationSnapshotInterval("myprop/clust/ns1");

        namespaces.run(split("set-dispatcher-pause-on-ack-state-persistent myprop/clust/ns1"));
        verify(mockNamespaces).setDispatcherPauseOnAckStatePersistent("myprop/clust/ns1");

        namespaces.run(split("get-dispatcher-pause-on-ack-state-persistent myprop/clust/ns1"));
        verify(mockNamespaces).getDispatcherPauseOnAckStatePersistent("myprop/clust/ns1");

        namespaces.run(split("remove-dispatcher-pause-on-ack-state-persistent myprop/clust/ns1"));
        verify(mockNamespaces).removeDispatcherPauseOnAckStatePersistent("myprop/clust/ns1");

    }

    @Test
    public void namespacesCreateV1() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Namespaces mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        CmdNamespaces namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("create my-prop/my-cluster/my-namespace"));
        verify(mockNamespaces).createNamespace("my-prop/my-cluster/my-namespace");
    }

    @Test
    public void namespacesCreateV1WithBundlesAndClusters() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Namespaces mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        CmdNamespaces namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("create my-prop/my-cluster/my-namespace --bundles 5 --clusters a,b,c"));
        verify(mockNamespaces).createNamespace("my-prop/my-cluster/my-namespace", 5);
        verify(mockNamespaces).setNamespaceReplicationClusters("my-prop/my-cluster/my-namespace",
                Sets.newHashSet("a", "b", "c"));
    }

    @Test
    public void namespacesCreate() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Namespaces mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        CmdNamespaces namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("create my-prop/my-namespace"));

        Policies policies = new Policies();
        policies.bundles = null;
        verify(mockNamespaces).createNamespace("my-prop/my-namespace", policies);
    }

    @Test
    public void namespacesCreateWithBundlesAndClusters() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Namespaces mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        CmdNamespaces namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split("create my-prop/my-namespace --bundles 5 --clusters a,b,c"));

        Policies policies = new Policies();
        policies.bundles = BundlesData.builder().numBundles(5).build();
        policies.replication_clusters = Sets.newHashSet("a", "b", "c");
        verify(mockNamespaces).createNamespace("my-prop/my-namespace", policies);
    }

    @Test
    public void resourceQuotas() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        ResourceQuotas mockResourceQuotas = mock(ResourceQuotas.class);
        when(admin.resourceQuotas()).thenReturn(mockResourceQuotas);
        CmdResourceQuotas cmdResourceQuotas = new CmdResourceQuotas(() -> admin);

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
        cmdResourceQuotas = new CmdResourceQuotas(() -> admin);

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
    public void namespaceIsolationPolicy() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Clusters mockClusters = mock(Clusters.class);
        when(admin.clusters()).thenReturn(mockClusters);

        CmdNamespaceIsolationPolicy nsIsolationPoliciesCmd = new CmdNamespaceIsolationPolicy(() -> admin);

        nsIsolationPoliciesCmd.run(split("brokers use"));
        verify(mockClusters).getBrokersWithNamespaceIsolationPolicy("use");

        nsIsolationPoliciesCmd.run(split("broker use --broker my-broker"));
        verify(mockClusters).getBrokerWithNamespaceIsolationPolicy("use", "my-broker");
    }

    @Test
    public void topicPolicies() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        TopicPolicies mockTopicsPolicies = mock(TopicPolicies.class);
        TopicPolicies mockGlobalTopicsPolicies = mock(TopicPolicies.class);
        when(admin.topicPolicies()).thenReturn(mockTopicsPolicies);
        when(admin.topicPolicies(false)).thenReturn(mockTopicsPolicies);
        when(admin.topicPolicies(true)).thenReturn(mockGlobalTopicsPolicies);
        Schemas mockSchemas = mock(Schemas.class);
        when(admin.schemas()).thenReturn(mockSchemas);
        Lookup mockLookup = mock(Lookup.class);
        when(admin.lookups()).thenReturn(mockLookup);

        CmdTopicPolicies cmdTopics = new CmdTopicPolicies(() -> admin);

        cmdTopics.run(split("set-subscription-types-enabled persistent://myprop/clust/ns1/ds1 -t Shared,Failover"));
        verify(mockTopicsPolicies).setSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1",
                Sets.newHashSet(SubscriptionType.Shared, SubscriptionType.Failover));
        cmdTopics.run(split("get-subscription-types-enabled persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("remove-subscription-types-enabled persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-offload-policies persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getOffloadPolicies("persistent://myprop/clust/ns1/ds1", false);

        cmdTopics.run(split("remove-offload-policies persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeOffloadPolicies("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-offload-policies persistent://myprop/clust/ns1/ds1 -d s3 -r" +
                " region -b bucket -e endpoint -m 8 -rb 9 -t 10 -ts 10 -orp tiered-storage-first"));
        verify(mockTopicsPolicies)
                .setOffloadPolicies("persistent://myprop/clust/ns1/ds1",
                        OffloadPoliciesImpl.create("s3", "region", "bucket" , "endpoint", null, null, null, null,
                                8, 9, 10L, 10L, null, OffloadedReadPriority.TIERED_STORAGE_FIRST));

        cmdTopics.run(split("get-retention persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getRetention("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-retention persistent://myprop/clust/ns1/ds1 -t 10m -s 20M"));
        verify(mockTopicsPolicies).setRetention("persistent://myprop/clust/ns1/ds1",
                new RetentionPolicies(10, 20));

        // Test with default time unit (seconds)
        cmdTopics = new CmdTopicPolicies(() -> admin);
        reset(mockTopicsPolicies);
        cmdTopics.run(split("set-retention persistent://myprop/clust/ns1/ds1 -t 180 -s 20M"));
        verify(mockTopicsPolicies).setRetention("persistent://myprop/clust/ns1/ds1",
                new RetentionPolicies(3, 20));

        // Test with explicit time unit (seconds)
        cmdTopics = new CmdTopicPolicies(() -> admin);
        reset(mockTopicsPolicies);
        cmdTopics.run(split("set-retention persistent://myprop/clust/ns1/ds1 -t 180s -s 20M"));
        verify(mockTopicsPolicies).setRetention("persistent://myprop/clust/ns1/ds1",
                new RetentionPolicies(3, 20));

        // Test size with default size less than 1 mb
        cmdTopics = new CmdTopicPolicies(() -> admin);
        reset(mockTopicsPolicies);
        cmdTopics.run(split("set-retention persistent://myprop/clust/ns1/ds1 -t 180 -s 4096"));
        verify(mockTopicsPolicies).setRetention("persistent://myprop/clust/ns1/ds1",
                new RetentionPolicies(3, 0));

        // Test size with default size greater than 1mb
        cmdTopics = new CmdTopicPolicies(() -> admin);
        reset(mockTopicsPolicies);
        cmdTopics.run(split("set-retention persistent://myprop/clust/ns1/ds1 -t 180 -s " + (2 * 1024 * 1024)));
        verify(mockTopicsPolicies).setRetention("persistent://myprop/clust/ns1/ds1",
                new RetentionPolicies(3, 2));

        cmdTopics.run(split("remove-retention persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeRetention("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-inactive-topic-policies persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1",false);
        cmdTopics.run(split("remove-inactive-topic-policies persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-inactive-topic-policies persistent://myprop/clust/ns1/ds1"
                + " -e -t 1s -m delete_when_no_subscriptions"));
        verify(mockTopicsPolicies).setInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1"
                , new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true));
        cmdTopics.run(split("get-compaction-threshold persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getCompactionThreshold("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-compaction-threshold persistent://myprop/clust/ns1/ds1 -t 10k"));
        verify(mockTopicsPolicies).setCompactionThreshold("persistent://myprop/clust/ns1/ds1", 10 * 1024);
        cmdTopics.run(split("remove-compaction-threshold persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeCompactionThreshold("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-producers persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getMaxProducers("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-producers persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeMaxProducers("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-producers persistent://myprop/clust/ns1/ds1 -p 99"));
        verify(mockTopicsPolicies).setMaxProducers("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-dispatch-rate persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopicsPolicies).getDispatchRate("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("remove-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeDispatchRate("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-dispatch-rate persistent://myprop/clust/ns1/ds1 -md -1 -bd -1 -dt 2"));
        verify(mockTopicsPolicies).setDispatchRate("persistent://myprop/clust/ns1/ds1", DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(2)
                .build());

        cmdTopics.run(split("set-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1 -md -1 -bd -1 -dt 2"));
        verify(mockTopicsPolicies).setReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1",
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(-1)
                        .dispatchThrottlingRateInByte(-1)
                        .ratePeriodInSecond(2)
                        .build());
        cmdTopics.run(split("get-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -md -1 -bd -1 -dt 2"));
        verify(mockTopicsPolicies).setSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1",
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(-1)
                        .dispatchThrottlingRateInByte(-1)
                        .ratePeriodInSecond(2)
                        .build());
        cmdTopics.run(split("get-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics = new CmdTopicPolicies(() -> admin);
        cmdTopics.run(split("set-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -s sub -md -1 -bd -1 -dt 3"));
        verify(mockTopicsPolicies).setSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1", "sub",
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(-1)
                        .dispatchThrottlingRateInByte(-1)
                        .ratePeriodInSecond(3)
                        .build());
        cmdTopics.run(split("get-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -s sub"));
        verify(mockTopicsPolicies).getSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1", "sub",false);
        cmdTopics.run(split("remove-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -s sub"));
        verify(mockTopicsPolicies).removeSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1", "sub");

        cmdTopics.run(split("get-persistence persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getPersistence("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-persistence persistent://myprop/clust/ns1/ds1 -e 2 -w 1 -a 1 -r 100.0"));
        verify(mockTopicsPolicies).setPersistence("persistent://myprop/clust/ns1/ds1",
                new PersistencePolicies(2, 1, 1, 100.0d));
        cmdTopics.run(split("remove-persistence persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removePersistence("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-publish-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getPublishRate("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-publish-rate persistent://myprop/clust/ns1/ds1 -m 10 -b 100"));
        verify(mockTopicsPolicies).setPublishRate("persistent://myprop/clust/ns1/ds1", new PublishRate(10, 100));
        cmdTopics.run(split("remove-publish-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removePublishRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-subscribe-rate persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopicsPolicies).getSubscribeRate("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("set-subscribe-rate persistent://myprop/clust/ns1/ds1 -sr 10 -st 100"));
        verify(mockTopicsPolicies).setSubscribeRate("persistent://myprop/clust/ns1/ds1", new SubscribeRate(10, 100));
        cmdTopics.run(split("remove-subscribe-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeSubscribeRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-message-size persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getMaxMessageSize("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-message-size persistent://myprop/clust/ns1/ds1 -m 1000"));
        verify(mockTopicsPolicies).setMaxMessageSize("persistent://myprop/clust/ns1/ds1", 1000);
        cmdTopics.run(split("remove-max-message-size persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeMaxMessageSize("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-consumers persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getMaxConsumers("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-consumers persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeMaxConsumers("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-consumers persistent://myprop/clust/ns1/ds1 -c 99"));
        verify(mockTopicsPolicies).setMaxConsumers("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("remove-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies, times(1)).removeMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("get-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies, times(1))
                .getMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1 -m 999"));
        verify(mockTopicsPolicies, times(1)).setMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", 999);

        cmdTopics.run(split("get-message-ttl persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getMessageTTL("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-message-ttl persistent://myprop/clust/ns1/ds1 -t 10"));
        verify(mockTopicsPolicies).setMessageTTL("persistent://myprop/clust/ns1/ds1", 10);
        cmdTopics.run(split("remove-message-ttl persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeMessageTTL("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1 -c 5"));
        verify(mockTopicsPolicies).setMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1", 5);
        cmdTopics.run(split("remove-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies, times(1)).getMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies, times(1)).removeMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1 -m 99"));
        verify(mockTopicsPolicies, times(1)).setMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-delayed-delivery persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-delayed-delivery persistent://myprop/clust/ns1/ds1 -t 10s --enable --maxDelay 5s"));
        verify(mockTopicsPolicies).setDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1",
                DelayedDeliveryPolicies.builder().tickTime(10000).active(true).maxDeliveryDelayInMillis(5000).build());
        cmdTopics.run(split("remove-delayed-delivery persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1") ;

        cmdTopics.run(split("get-deduplication persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getDeduplicationStatus("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-deduplication persistent://myprop/clust/ns1/ds1 --disable"));
        verify(mockTopicsPolicies).setDeduplicationStatus("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-deduplication persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeDeduplicationStatus("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-subscriptions-per-topic persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-subscriptions-per-topic persistent://myprop/clust/ns1/ds1 -s 1024"));
        verify(mockTopicsPolicies).setMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1", 1024);
        cmdTopics.run(split("remove-max-subscriptions-per-topic persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).getDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1 -i 100"));
        verify(mockTopicsPolicies).setDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1", 100);
        cmdTopics.run(split("remove-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1");

        // Reset the cmd, and check global option
        cmdTopics = new CmdTopicPolicies(() -> admin);
        cmdTopics.run(split("get-retention persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getRetention("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-retention persistent://myprop/clust/ns1/ds1 -t 10m -s 20M -g"));
        verify(mockGlobalTopicsPolicies).setRetention("persistent://myprop/clust/ns1/ds1",
                new RetentionPolicies(10, 20));

        cmdTopics = new CmdTopicPolicies(() -> admin);
        cmdTopics.run(split("set-retention persistent://myprop/clust/ns1/ds1 -t 1440s -s 20M -g"));
        verify(mockGlobalTopicsPolicies).setRetention("persistent://myprop/clust/ns1/ds1",
                new RetentionPolicies(24, 20));

        cmdTopics = new CmdTopicPolicies(() -> admin);
        reset(mockGlobalTopicsPolicies);
        cmdTopics.run(split("set-retention persistent://myprop/clust/ns1/ds1 -t 1440 -s 20M -g"));
        verify(mockGlobalTopicsPolicies).setRetention("persistent://myprop/clust/ns1/ds1",
                new RetentionPolicies(24, 20));

        cmdTopics.run(split("remove-retention persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeRetention("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-backlog-quota persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopicsPolicies).getBacklogQuotaMap("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("set-backlog-quota persistent://myprop/clust/ns1/ds1 -l 10 -p producer_request_hold"));
        verify(mockTopicsPolicies).setBacklogQuota("persistent://myprop/clust/ns1/ds1",
                BacklogQuota.builder()
                        .limitSize(10)
                        .retentionPolicy(RetentionPolicy.producer_request_hold)
                        .build(),
                BacklogQuota.BacklogQuotaType.destination_storage);
        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopicPolicies(() -> admin);
        cmdTopics.run(split("set-message-ttl persistent://myprop/clust/ns1/ds1 -t 10h"));
        verify(mockTopicsPolicies).setMessageTTL("persistent://myprop/clust/ns1/ds1", 10 * 60 * 60);
        cmdTopics.run(split("set-backlog-quota persistent://myprop/clust/ns1/ds1 -lt 1w -p consumer_backlog_eviction -t message_age"));
        verify(mockTopicsPolicies).setBacklogQuota("persistent://myprop/clust/ns1/ds1",
                BacklogQuota.builder()
                        .limitTime(60 * 60 * 24 * 7)
                        .retentionPolicy(RetentionPolicy.consumer_backlog_eviction)
                        .build(),
                BacklogQuota.BacklogQuotaType.message_age);
        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopicPolicies(() -> admin);
        cmdTopics.run(split("set-backlog-quota persistent://myprop/clust/ns1/ds1 -lt 1000 -p producer_request_hold -t message_age"));
        verify(mockTopicsPolicies).setBacklogQuota("persistent://myprop/clust/ns1/ds1",
                BacklogQuota.builder()
                        .limitTime(1000)
                        .retentionPolicy(RetentionPolicy.producer_request_hold)
                        .build(),
                BacklogQuota.BacklogQuotaType.message_age);
        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopicPolicies(() -> admin);
        Assert.assertFalse(cmdTopics.run(split("set-backlog-quota persistent://myprop/clust/ns1/ds1 -l 1000 -p producer_request_hold -t message_age")));
        cmdTopics = new CmdTopicPolicies(() -> admin);
        Assert.assertFalse(cmdTopics.run(split("set-backlog-quota persistent://myprop/clust/ns1/ds1 -lt 60 -p producer_request_hold -t destination_storage")));

        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopicPolicies(() -> admin);
        cmdTopics.run(split("remove-backlog-quota persistent://myprop/clust/ns1/ds1"));
        verify(mockTopicsPolicies).removeBacklogQuota("persistent://myprop/clust/ns1/ds1", BacklogQuota.BacklogQuotaType.destination_storage);
        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopicPolicies(() -> admin);
        cmdTopics.run(split("remove-backlog-quota persistent://myprop/clust/ns1/ds1 -t message_age"));
        verify(mockTopicsPolicies).removeBacklogQuota("persistent://myprop/clust/ns1/ds1", BacklogQuota.BacklogQuotaType.message_age);

        cmdTopics.run(split("get-max-producers persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getMaxProducers("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-producers persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeMaxProducers("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-producers persistent://myprop/clust/ns1/ds1 -p 99 -g"));
        verify(mockGlobalTopicsPolicies).setMaxProducers("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("remove-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies, times(1))
                .removeMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("get-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies, times(1))
                .getMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1 -m 999 -g"));
        verify(mockGlobalTopicsPolicies, times(1)).setMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", 999);

        cmdTopics.run(split("get-message-ttl persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getMessageTTL("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-message-ttl persistent://myprop/clust/ns1/ds1 -t 10 -g"));
        verify(mockGlobalTopicsPolicies).setMessageTTL("persistent://myprop/clust/ns1/ds1", 10);
        cmdTopics.run(split("remove-message-ttl persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeMessageTTL("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-persistence persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getPersistence("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-persistence persistent://myprop/clust/ns1/ds1 -e 2 -w 1 -a 1 -r 100.0 -g"));
        verify(mockGlobalTopicsPolicies).setPersistence("persistent://myprop/clust/ns1/ds1",
                new PersistencePolicies(2, 1, 1, 100.0d));
        cmdTopics.run(split("remove-persistence persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removePersistence("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies, times(1)).getMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies, times(1)).removeMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1 -m 99 -g"));
        verify(mockGlobalTopicsPolicies, times(1)).setMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-inactive-topic-policies persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1",false);
        cmdTopics.run(split("remove-inactive-topic-policies persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-inactive-topic-policies persistent://myprop/clust/ns1/ds1"
                + " -e -t 1s -m delete_when_no_subscriptions -g"));
        verify(mockGlobalTopicsPolicies).setInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1"
                , new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true));

        cmdTopics.run(split("get-dispatch-rate persistent://myprop/clust/ns1/ds1 -ap -g"));
        verify(mockGlobalTopicsPolicies).getDispatchRate("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("remove-dispatch-rate persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeDispatchRate("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-dispatch-rate persistent://myprop/clust/ns1/ds1 -md -1 -bd -1 -dt 2 -g"));
        verify(mockGlobalTopicsPolicies).setDispatchRate("persistent://myprop/clust/ns1/ds1", DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(2)
                .build());

        cmdTopics.run(split("get-publish-rate persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getPublishRate("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-publish-rate persistent://myprop/clust/ns1/ds1 -m 10 -b 100 -g"));
        verify(mockGlobalTopicsPolicies).setPublishRate("persistent://myprop/clust/ns1/ds1", new PublishRate(10, 100));
        cmdTopics.run(split("remove-publish-rate persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removePublishRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-subscribe-rate persistent://myprop/clust/ns1/ds1 -ap -g"));
        verify(mockGlobalTopicsPolicies).getSubscribeRate("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("set-subscribe-rate persistent://myprop/clust/ns1/ds1 -sr 10 -st 100 -g"));
        verify(mockGlobalTopicsPolicies).setSubscribeRate("persistent://myprop/clust/ns1/ds1", new SubscribeRate(10, 100));
        cmdTopics.run(split("remove-subscribe-rate persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeSubscribeRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-delayed-delivery persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-delayed-delivery persistent://myprop/clust/ns1/ds1 -t 10s --enable -md 5s -g"));
        verify(mockGlobalTopicsPolicies).setDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1",
                DelayedDeliveryPolicies.builder().tickTime(10000).active(true).maxDeliveryDelayInMillis(5000).build());
        cmdTopics.run(split("remove-delayed-delivery persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1") ;

        cmdTopics.run(split("get-max-message-size persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getMaxMessageSize("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-message-size persistent://myprop/clust/ns1/ds1 -m 1000 -g"));
        verify(mockGlobalTopicsPolicies).setMaxMessageSize("persistent://myprop/clust/ns1/ds1", 1000);
        cmdTopics.run(split("remove-max-message-size persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeMaxMessageSize("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-deduplication persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getDeduplicationStatus("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-deduplication persistent://myprop/clust/ns1/ds1 --disable -g"));
        verify(mockGlobalTopicsPolicies).setDeduplicationStatus("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-deduplication persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeDeduplicationStatus("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1 -i 100 -g"));
        verify(mockGlobalTopicsPolicies).setDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1", 100);
        cmdTopics.run(split("remove-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1 -c 5 -g"));
        verify(mockGlobalTopicsPolicies).setMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1", 5);
        cmdTopics.run(split("remove-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-subscription-types-enabled persistent://myprop/clust/ns1/ds1 -t Shared,Failover -g"));
        verify(mockGlobalTopicsPolicies).setSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1",
                Sets.newHashSet(SubscriptionType.Shared, SubscriptionType.Failover));
        cmdTopics.run(split("get-subscription-types-enabled persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("remove-subscription-types-enabled persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-consumers persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getMaxConsumers("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-consumers persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeMaxConsumers("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-consumers persistent://myprop/clust/ns1/ds1 -c 99 -g"));
        verify(mockGlobalTopicsPolicies).setMaxConsumers("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-compaction-threshold persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getCompactionThreshold("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-compaction-threshold persistent://myprop/clust/ns1/ds1 -t 10k -g"));
        verify(mockGlobalTopicsPolicies).setCompactionThreshold("persistent://myprop/clust/ns1/ds1", 10 * 1024);
        cmdTopics.run(split("remove-compaction-threshold persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeCompactionThreshold("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1 -md -1 -bd -1 -dt 2 -g"));
        verify(mockGlobalTopicsPolicies).setReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1",
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(-1)
                        .dispatchThrottlingRateInByte(-1)
                        .ratePeriodInSecond(2)
                        .build());
        cmdTopics.run(split("get-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -md -1 -bd -1 -dt 2 -g"));
        verify(mockGlobalTopicsPolicies).setSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1",
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(-1)
                        .dispatchThrottlingRateInByte(-1)
                        .ratePeriodInSecond(2)
                        .build());
        cmdTopics.run(split("get-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics = new CmdTopicPolicies(() -> admin);
        cmdTopics.run(split("set-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -s sub -md -1 -bd -1 -dt 2 -g"));
        verify(mockGlobalTopicsPolicies).setSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1", "sub",
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(-1)
                        .dispatchThrottlingRateInByte(-1)
                        .ratePeriodInSecond(2)
                        .build());
        cmdTopics.run(split("get-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -s sub -g"));
        verify(mockGlobalTopicsPolicies).getSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1", "sub",false);
        cmdTopics.run(split("remove-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -s sub -g"));
        verify(mockGlobalTopicsPolicies).removeSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1","sub");

        cmdTopics.run(split("get-max-subscriptions-per-topic persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-subscriptions-per-topic persistent://myprop/clust/ns1/ds1 -s 1024 -g"));
        verify(mockGlobalTopicsPolicies).setMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1", 1024);
        cmdTopics.run(split("remove-max-subscriptions-per-topic persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-offload-policies persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).getOffloadPolicies("persistent://myprop/clust/ns1/ds1", false);

        cmdTopics.run(split("remove-offload-policies persistent://myprop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeOffloadPolicies("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-offload-policies persistent://myprop/clust/ns1/ds1 -d s3 -r" +
                " region -b bucket -e endpoint -m 8 -rb 9 -t 10 -ts 100 -orp tiered-storage-first -g"));
        verify(mockGlobalTopicsPolicies)
                .setOffloadPolicies("persistent://myprop/clust/ns1/ds1",
                        OffloadPoliciesImpl.create("s3", "region", "bucket" , "endpoint", null, null, null, null,
                                8, 9, 10L, 100L, null, OffloadedReadPriority.TIERED_STORAGE_FIRST));

        cmdTopics.run(split("set-auto-subscription-creation persistent://prop/clust/ns1/ds1 -e -g"));
        verify(mockGlobalTopicsPolicies).setAutoSubscriptionCreation("persistent://prop/clust/ns1/ds1",
                AutoSubscriptionCreationOverride.builder()
                        .allowAutoSubscriptionCreation(true)
                        .build());
        cmdTopics.run(split("get-auto-subscription-creation persistent://prop/clust/ns1/ds1 -a -g"));
        verify(mockGlobalTopicsPolicies).getAutoSubscriptionCreation("persistent://prop/clust/ns1/ds1", true);
        cmdTopics.run(split("remove-auto-subscription-creation persistent://prop/clust/ns1/ds1 -g"));
        verify(mockGlobalTopicsPolicies).removeAutoSubscriptionCreation("persistent://prop/clust/ns1/ds1");
    }

    @Test
    public void topicsSetOffloadPolicies() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Topics mockTopics = mock(Topics.class);
        when(admin.topics()).thenReturn(mockTopics);
        Schemas mockSchemas = mock(Schemas.class);
        when(admin.schemas()).thenReturn(mockSchemas);
        Lookup mockLookup = mock(Lookup.class);
        when(admin.lookups()).thenReturn(mockLookup);

        // filesystem offload
        CmdTopics cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("set-offload-policies persistent://myprop/clust/ns1/ds1 -d filesystem -oat 100M -oats 1h -oae 1h -orp bookkeeper-first"));
        OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create("filesystem", null, null
          , null, null, null, null, null, 64 * 1024 * 1024, 1024 * 1024,
          100 * 1024 * 1024L, 3600L, 3600 * 1000L, OffloadedReadPriority.BOOKKEEPER_FIRST);
        verify(mockTopics).setOffloadPolicies("persistent://myprop/clust/ns1/ds1", offloadPolicies);

//         S3 offload
        CmdTopics cmdTopics2 = new CmdTopics(() -> admin);
        cmdTopics2.run(split("set-offload-policies persistent://myprop/clust/ns1/ds2 -d s3 -r region -b bucket -e endpoint -ts 50 -m 8 -rb 9 -t 10 -orp tiered-storage-first"));
        OffloadPoliciesImpl offloadPolicies2 = OffloadPoliciesImpl.create("s3", "region", "bucket"
          , "endpoint", null, null, null, null,
          8, 9, 10L, 50L, null, OffloadedReadPriority.TIERED_STORAGE_FIRST);
        verify(mockTopics).setOffloadPolicies("persistent://myprop/clust/ns1/ds2", offloadPolicies2);
    }


    @Test
    public void topics() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Topics mockTopics = mock(Topics.class);
        when(admin.topics()).thenReturn(mockTopics);
        Schemas mockSchemas = mock(Schemas.class);
        when(admin.schemas()).thenReturn(mockSchemas);
        Lookup mockLookup = mock(Lookup.class);
        when(admin.lookups()).thenReturn(mockLookup);

        CmdTopics cmdTopics = new CmdTopics(() -> admin);

        cmdTopics.run(split("truncate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).truncate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("delete persistent://myprop/clust/ns1/ds1 -f"));
        verify(mockTopics).delete("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("unload persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).unload("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("permissions persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getPermissions("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("grant-permission persistent://myprop/clust/ns1/ds1 --role admin --actions produce,consume"));
        verify(mockTopics).grantPermission("persistent://myprop/clust/ns1/ds1", "admin", Sets.newHashSet(AuthAction.produce, AuthAction.consume));

        cmdTopics.run(split("revoke-permission persistent://myprop/clust/ns1/ds1 --role admin"));
        verify(mockTopics).revokePermissions("persistent://myprop/clust/ns1/ds1", "admin");

        cmdTopics.run(split("list myprop/clust/ns1"));
        verify(mockTopics).getList("myprop/clust/ns1", null, ListTopicsOptions.EMPTY);

        cmdTopics.run(split("lookup persistent://myprop/clust/ns1/ds1"));
        verify(mockLookup).lookupTopic("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("partitioned-lookup persistent://myprop/clust/ns1/ds1"));
        verify(mockLookup).lookupPartitionedTopic("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("partitioned-lookup persistent://myprop/clust/ns1/ds1 --sort-by-broker"));
        verify(mockLookup, times(2)).lookupPartitionedTopic("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("bundle-range persistent://myprop/clust/ns1/ds1"));
        verify(mockLookup).getBundleRange("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("subscriptions persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getSubscriptions("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("unsubscribe persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).deleteSubscription("persistent://myprop/clust/ns1/ds1", "sub1", false);

        cmdTopics.run(split("stats persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getStats("persistent://myprop/clust/ns1/ds1", false, true, false);

        cmdTopics.run(split("stats-internal persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getInternalStats("persistent://myprop/clust/ns1/ds1", false);

        cmdTopics.run(split("get-backlog-quotas persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getBacklogQuotaMap("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("set-backlog-quota persistent://myprop/clust/ns1/ds1 -l 10 -p producer_request_hold"));
        verify(mockTopics).setBacklogQuota("persistent://myprop/clust/ns1/ds1",
                BacklogQuota.builder()
                        .limitSize(10)
                        .retentionPolicy(RetentionPolicy.producer_request_hold)
                        .build(),
                BacklogQuota.BacklogQuotaType.destination_storage);
        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("set-backlog-quota persistent://myprop/clust/ns1/ds1 -lt 5h -p consumer_backlog_eviction"));
        verify(mockTopics).setBacklogQuota("persistent://myprop/clust/ns1/ds1",
                BacklogQuota.builder()
                        .limitSize(-1)
                        .limitTime(5 * 60 * 60)
                        .retentionPolicy(RetentionPolicy.consumer_backlog_eviction)
                        .build(),
                BacklogQuota.BacklogQuotaType.destination_storage);
        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("set-backlog-quota persistent://myprop/clust/ns1/ds1 -lt 1000 -p producer_request_hold -t message_age"));
        verify(mockTopics).setBacklogQuota("persistent://myprop/clust/ns1/ds1",
                BacklogQuota.builder()
                        .limitSize(-1)
                        .limitTime(1000)
                        .retentionPolicy(RetentionPolicy.producer_request_hold)
                        .build(),
                BacklogQuota.BacklogQuotaType.message_age);
        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("remove-backlog-quota persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeBacklogQuota("persistent://myprop/clust/ns1/ds1", BacklogQuota.BacklogQuotaType.destination_storage);
        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("remove-backlog-quota persistent://myprop/clust/ns1/ds1 -t message_age"));
        verify(mockTopics).removeBacklogQuota("persistent://myprop/clust/ns1/ds1", BacklogQuota.BacklogQuotaType.message_age);

        cmdTopics.run(split("info-internal persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getInternalInfo("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("partitioned-stats persistent://myprop/clust/ns1/ds1 --per-partition"));
        verify(mockTopics).getPartitionedStats("persistent://myprop/clust/ns1/ds1",
                true, false, true, false);

        cmdTopics.run(split("partitioned-stats-internal persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getPartitionedInternalStats("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("clear-backlog persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).skipAllMessages("persistent://myprop/clust/ns1/ds1", "sub1");

        cmdTopics.run(split("skip persistent://myprop/clust/ns1/ds1 -s sub1 -n 100"));
        verify(mockTopics).skipMessages("persistent://myprop/clust/ns1/ds1", "sub1", 100);

        cmdTopics.run(split("expire-messages persistent://myprop/clust/ns1/ds1 -s sub1 -t 100"));
        verify(mockTopics).expireMessages("persistent://myprop/clust/ns1/ds1", "sub1", 100);

        cmdTopics.run(split("expire-messages-all-subscriptions persistent://myprop/clust/ns1/ds1 -t 100"));
        verify(mockTopics).expireMessagesForAllSubscriptions("persistent://myprop/clust/ns1/ds1", 100);

        cmdTopics.run(split("get-subscribe-rate persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getSubscribeRate("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("set-subscribe-rate persistent://myprop/clust/ns1/ds1 -sr 2 -st 60"));
        verify(mockTopics).setSubscribeRate("persistent://myprop/clust/ns1/ds1", new SubscribeRate(2, 60));

        cmdTopics.run(split("remove-subscribe-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeSubscribeRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-replicated-subscription-status persistent://myprop/clust/ns1/ds1 -s sub1 -e"));
        verify(mockTopics).setReplicatedSubscriptionStatus("persistent://myprop/clust/ns1/ds1", "sub1", true);

        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("expire-messages persistent://myprop/clust/ns1/ds1 -s sub1 -p 1:1 -e"));
        verify(mockTopics).expireMessages(eq("persistent://myprop/clust/ns1/ds1"), eq("sub1"), eq(new MessageIdImpl(1, 1, -1)), eq(true));

        cmdTopics.run(split("expire-messages-all-subscriptions persistent://myprop/clust/ns1/ds1 -t 1d"));
        verify(mockTopics).expireMessagesForAllSubscriptions("persistent://myprop/clust/ns1/ds1", 60 * 60 * 24);

        cmdTopics.run(split("create-subscription persistent://myprop/clust/ns1/ds1 -s sub1 --messageId earliest"));
        verify(mockTopics).createSubscription("persistent://myprop/clust/ns1/ds1", "sub1", MessageId.earliest, false, null);

        cmdTopics.run(split("analyze-backlog persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).analyzeSubscriptionBacklog("persistent://myprop/clust/ns1/ds1", "sub1", Optional.empty());

        cmdTopics.run(split("trim-topic persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).trimTopic("persistent://myprop/clust/ns1/ds1");

        // jcommander is stateful, you cannot parse the same command twice
        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("create-subscription persistent://myprop/clust/ns1/ds1 -s sub1 --messageId earliest --property a=b -p x=y,z"));
        Map<String, String> props = new HashMap<>();
        props.put("a", "b");
        props.put("x", "y,z");
        verify(mockTopics).createSubscription("persistent://myprop/clust/ns1/ds1", "sub1", MessageId.earliest, false, props);

        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("create-subscription persistent://myprop/clust/ns1/ds1 -s sub1 --messageId earliest -r"));
        verify(mockTopics).createSubscription("persistent://myprop/clust/ns1/ds1", "sub1", MessageId.earliest, true, null);

        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("update-subscription-properties persistent://myprop/clust/ns1/ds1 -s sub1 --clear"));
        verify(mockTopics).updateSubscriptionProperties("persistent://myprop/clust/ns1/ds1", "sub1", new HashMap<>());

        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("update-properties persistent://myprop/clust/ns1/ds1 --property a=b -p x=y,z"));
        props = new HashMap<>();
        props.put("a", "b");
        props.put("x", "y,z");
        verify(mockTopics).updateProperties("persistent://myprop/clust/ns1/ds1", props);

        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("remove-properties persistent://myprop/clust/ns1/ds1 --key a"));
        verify(mockTopics).removeProperties("persistent://myprop/clust/ns1/ds1", "a");

        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("get-subscription-properties persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).getSubscriptionProperties("persistent://myprop/clust/ns1/ds1", "sub1");

        cmdTopics = new CmdTopics(() -> admin);
        props = new HashMap<>();
        props.put("a", "b");
        props.put("c", "d");
        props.put("x", "y,z");
        cmdTopics.run(split("update-subscription-properties persistent://myprop/clust/ns1/ds1 -s sub1 -p a=b -p c=d -p x=y,z"));
        verify(mockTopics).updateSubscriptionProperties("persistent://myprop/clust/ns1/ds1", "sub1", props);

        cmdTopics.run(split("create-partitioned-topic persistent://myprop/clust/ns1/ds1 --partitions 32"));
        verify(mockTopics).createPartitionedTopic("persistent://myprop/clust/ns1/ds1", 32, null);

        cmdTopics.run(split("create-missed-partitions persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).createMissedPartitions("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("create persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).createNonPartitionedTopic("persistent://myprop/clust/ns1/ds1", null);

        cmdTopics.run(split("list-partitioned-topics myprop/clust/ns1"));
        verify(mockTopics).getPartitionedTopicList("myprop/clust/ns1", ListTopicsOptions.EMPTY);

        cmdTopics.run(split("update-partitioned-topic persistent://myprop/clust/ns1/ds1 -p 6"));
        verify(mockTopics).updatePartitionedTopic("persistent://myprop/clust/ns1/ds1", 6, false, false);

        cmdTopics.run(split("get-partitioned-topic-metadata persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getPartitionedTopicMetadata("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("delete-partitioned-topic persistent://myprop/clust/ns1/ds1 -f"));
        verify(mockTopics).deletePartitionedTopic("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("peek-messages persistent://myprop/clust/ns1/ds1 -s sub1 -n 3"));
        verify(mockTopics).peekMessages("persistent://myprop/clust/ns1/ds1", "sub1", 3,
                false, TransactionIsolationLevel.READ_COMMITTED);

        MessageImpl message = mock(MessageImpl.class);
        when(message.getData()).thenReturn(new byte[]{});
        when(message.getMessageId()).thenReturn(new MessageIdImpl(1L, 1L, 1));
        when(mockTopics.examineMessage("persistent://myprop/clust/ns1/ds1", "latest", 1)).thenReturn(message);
        cmdTopics.run(split("examine-messages persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).examineMessage("persistent://myprop/clust/ns1/ds1", "latest", 1);

        cmdTopics.run(split("enable-deduplication persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).enableDeduplication("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("disable-deduplication persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).enableDeduplication("persistent://myprop/clust/ns1/ds1", false);

        cmdTopics.run(split("set-deduplication persistent://myprop/clust/ns1/ds1 --disable"));
        verify(mockTopics).setDeduplicationStatus("persistent://myprop/clust/ns1/ds1", false);

        cmdTopics.run(split("set-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1 -md -1 -bd -1 -dt 2"));
        verify(mockTopics).setSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1",
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(-1)
                        .dispatchThrottlingRateInByte(-1)
                        .ratePeriodInSecond(2)
                        .build());
        cmdTopics.run(split("get-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-subscription-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeSubscriptionDispatchRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("remove-deduplication persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeDeduplicationStatus("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("set-subscription-types-enabled persistent://myprop/clust/ns1/ds1 -t Shared,Failover"));
        verify(mockTopics).setSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1",
                Sets.newHashSet(SubscriptionType.Shared, SubscriptionType.Failover));

        cmdTopics.run(split("get-subscription-types-enabled persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("remove-subscription-types-enabled persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeSubscriptionTypesEnabled("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1 -md 10 -bd 11 -dt 12"));
        verify(mockTopics).setReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1",
                DispatchRate.builder()
                        .dispatchThrottlingRateInMsg(10)
                        .dispatchThrottlingRateInByte(11)
                        .ratePeriodInSecond(12)
                        .build());

        cmdTopics.run(split("remove-replicator-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeReplicatorDispatchRate("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-deduplication-enabled persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getDeduplicationStatus("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("get-deduplication persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics, times(2)).getDeduplicationStatus("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-offload-policies persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getOffloadPolicies("persistent://myprop/clust/ns1/ds1", false);

        cmdTopics.run(split("remove-offload-policies persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeOffloadPolicies("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-delayed-delivery persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-delayed-delivery persistent://myprop/clust/ns1/ds1 -t 10s -md 5s --enable"));
        verify(mockTopics).setDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1",
                DelayedDeliveryPolicies.builder().tickTime(10000).active(true).maxDeliveryDelayInMillis(5000).build());
        cmdTopics.run(split("remove-delayed-delivery persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1") ;

        cmdTopics.run(split("set-offload-policies persistent://myprop/clust/ns1/ds1 -d s3 -r region -b bucket -e endpoint -ts 50 -m 8 -rb 9 -t 10 -orp tiered-storage-first"));
        OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create("s3", "region", "bucket"
                , "endpoint", null, null, null, null,
                8, 9, 10L, 50L, null, OffloadedReadPriority.TIERED_STORAGE_FIRST);
        verify(mockTopics).setOffloadPolicies("persistent://myprop/clust/ns1/ds1", offloadPolicies);

        cmdTopics.run(split("get-max-unacked-messages-on-consumer persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("get-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics, times(2))
                .getMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-unacked-messages-on-consumer persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("remove-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics, times(2)).removeMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-unacked-messages-on-consumer persistent://myprop/clust/ns1/ds1 -m 999"));
        verify(mockTopics).setMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", 999);
        cmdTopics.run(split("set-max-unacked-messages-per-consumer persistent://myprop/clust/ns1/ds1 -m 999"));
        verify(mockTopics, times(2)).setMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", 999);

        cmdTopics.run(split("get-max-unacked-messages-on-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("get-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics, times(2)).getMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-unacked-messages-on-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("remove-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics, times(2)).removeMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("get-publish-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getPublishRate("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-publish-rate persistent://myprop/clust/ns1/ds1 -m 100 -b 10240"));
        verify(mockTopics).setPublishRate("persistent://myprop/clust/ns1/ds1", new PublishRate(100, 10240L));
        cmdTopics.run(split("remove-publish-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removePublishRate("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-unacked-messages-on-subscription persistent://myprop/clust/ns1/ds1 -m 99"));
        verify(mockTopics).setMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", 99);
        cmdTopics.run(split("set-max-unacked-messages-per-subscription persistent://myprop/clust/ns1/ds1 -m 99"));
        verify(mockTopics, times(2)).setMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-compaction-threshold persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getCompactionThreshold("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("set-compaction-threshold persistent://myprop/clust/ns1/ds1 -t 10k"));
        verify(mockTopics).setCompactionThreshold("persistent://myprop/clust/ns1/ds1", 10 * 1024);
        cmdTopics.run(split("remove-compaction-threshold persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeCompactionThreshold("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-message-size persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getMaxMessageSize("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("remove-max-message-size persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeMaxMessageSize("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1 -c 5"));
        verify(mockTopics).setMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1", 5);

        cmdTopics.run(split("remove-max-consumers-per-subscription persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeMaxConsumersPerSubscription("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-max-message-size persistent://myprop/clust/ns1/ds1 -m 99"));
        verify(mockTopics).setMaxMessageSize("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-message-by-id persistent://myprop/clust/ns1/ds1 -l 10 -e 2"));
        verify(mockTopics).getMessageById("persistent://myprop/clust/ns1/ds1", 10,2);

        cmdTopics.run(split("get-dispatch-rate persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getDispatchRate("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("remove-dispatch-rate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeDispatchRate("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-dispatch-rate persistent://myprop/clust/ns1/ds1 -md -1 -bd -1 -dt 2"));
        verify(mockTopics).setDispatchRate("persistent://myprop/clust/ns1/ds1", DispatchRate.builder()
                .dispatchThrottlingRateInMsg(-1)
                .dispatchThrottlingRateInByte(-1)
                .ratePeriodInSecond(2)
                .build());

        cmdTopics.run(split("get-max-producers persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getMaxProducers("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-producers persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeMaxProducers("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-producers persistent://myprop/clust/ns1/ds1 -p 99"));
        verify(mockTopics).setMaxProducers("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-max-consumers persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getMaxConsumers("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-max-consumers persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeMaxConsumers("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-consumers persistent://myprop/clust/ns1/ds1 -c 99"));
        verify(mockTopics).setMaxConsumers("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("remove-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-deduplication-snapshot-interval persistent://myprop/clust/ns1/ds1 -i 99"));
        verify(mockTopics).setDeduplicationSnapshotInterval("persistent://myprop/clust/ns1/ds1", 99);

        cmdTopics.run(split("get-inactive-topic-policies persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1", false);
        cmdTopics.run(split("remove-inactive-topic-policies persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-inactive-topic-policies persistent://myprop/clust/ns1/ds1"
                        + " -e -t 1s -m delete_when_no_subscriptions"));
        verify(mockTopics).setInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1"
                , new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true));

        cmdTopics.run(split("get-max-subscriptions persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-max-subscriptions persistent://myprop/clust/ns1/ds1 -m 100"));
        verify(mockTopics).setMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1", 100);
        cmdTopics.run(split("remove-max-subscriptions persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeMaxSubscriptionsPerTopic("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-persistence persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getPersistence("persistent://myprop/clust/ns1/ds1");
        cmdTopics.run(split("set-persistence persistent://myprop/clust/ns1/ds1 -e 2 -w 1 -a 1 -r 100.0"));
        verify(mockTopics).setPersistence("persistent://myprop/clust/ns1/ds1", new PersistencePolicies(2, 1, 1, 100.0d));
        cmdTopics.run(split("remove-persistence persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removePersistence("persistent://myprop/clust/ns1/ds1");

        // argument matcher for the timestamp in reset cursor. Since we can't verify exact timestamp, we check for a
        // range of +/- 1 second of the expected timestamp
        class TimestampMatcher implements ArgumentMatcher<Long> {
            @Override
            public boolean matches(Long timestamp) {
                long expectedTimestamp = System.currentTimeMillis() - (1 * 60 * 1000);
                if (timestamp < (expectedTimestamp + 1000) && timestamp > (expectedTimestamp - 1000)) {
                    return true;
                }
                return false;
            }
        }
        cmdTopics.run(split("reset-cursor persistent://myprop/clust/ns1/ds1 -s sub1 -t 1m"));
        verify(mockTopics).resetCursor(eq("persistent://myprop/clust/ns1/ds1"), eq("sub1"),
                longThat(new TimestampMatcher()));

        when(mockTopics.terminateTopicAsync("persistent://myprop/clust/ns1/ds1")).thenReturn(CompletableFuture.completedFuture(new MessageIdImpl(1L, 1L, 1)));
        cmdTopics.run(split("terminate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).terminateTopicAsync("persistent://myprop/clust/ns1/ds1");

        Map<Integer, MessageId> results = new HashMap<>();
        results.put(0, new MessageIdImpl(1, 1, 0));
        when(mockTopics.terminatePartitionedTopic("persistent://myprop/clust/ns1/ds1")).thenReturn(results);
        cmdTopics.run(split("partitioned-terminate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).terminatePartitionedTopic("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("compact persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).triggerCompaction("persistent://myprop/clust/ns1/ds1");

        when(mockTopics.compactionStatus("persistent://myprop/clust/ns1/ds1")).thenReturn(new LongRunningProcessStatus());
        cmdTopics.run(split("compaction-status persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).compactionStatus("persistent://myprop/clust/ns1/ds1");

        PersistentTopicInternalStats stats = new PersistentTopicInternalStats();
        stats.ledgers = new ArrayList<>();
        stats.ledgers.add(newLedger(0, 10, 1000));
        stats.ledgers.add(newLedger(1, 10, 2000));
        stats.ledgers.add(newLedger(2, 10, 3000));
        when(mockTopics.getInternalStats("persistent://myprop/clust/ns1/ds1", false)).thenReturn(stats);
        cmdTopics.run(split("offload persistent://myprop/clust/ns1/ds1 -s 1k"));
        verify(mockTopics).triggerOffload("persistent://myprop/clust/ns1/ds1", 1024);

        when(mockTopics.offloadStatus("persistent://myprop/clust/ns1/ds1")).thenReturn(new OffloadProcessStatusImpl());
        cmdTopics.run(split("offload-status persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).offloadStatus("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("last-message-id persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getLastMessageId(eq("persistent://myprop/clust/ns1/ds1"));

        cmdTopics.run(split("get-message-ttl persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getMessageTTL("persistent://myprop/clust/ns1/ds1", false);

        cmdTopics.run(split("set-message-ttl persistent://myprop/clust/ns1/ds1 -t 10"));
        verify(mockTopics).setMessageTTL("persistent://myprop/clust/ns1/ds1", 10);

        cmdTopics.run(split("remove-message-ttl persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeMessageTTL("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-replicated-subscription-status persistent://myprop/clust/ns1/ds1 -s sub1 -d"));
        verify(mockTopics).setReplicatedSubscriptionStatus("persistent://myprop/clust/ns1/ds1", "sub1", false);

        cmdTopics.run(split("get-replicated-subscription-status persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).getReplicatedSubscriptionStatus("persistent://myprop/clust/ns1/ds1", "sub1");

        //cmd with option cannot be executed repeatedly.
        cmdTopics = new CmdTopics(() -> admin);

        cmdTopics.run(split("get-max-unacked-messages-on-subscription persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getMaxUnackedMessagesOnSubscription("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("reset-cursor persistent://myprop/clust/ns1/ds2 -s sub1 -m 1:1 -e"));
        verify(mockTopics).resetCursor(eq("persistent://myprop/clust/ns1/ds2"), eq("sub1")
                , eq(new MessageIdImpl(1, 1, -1)), eq(true));

        cmdTopics.run(split("get-maxProducers persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getMaxProducers("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("set-maxProducers persistent://myprop/clust/ns1/ds1 -p 3"));
        verify(mockTopics).setMaxProducers("persistent://myprop/clust/ns1/ds1", 3);

        cmdTopics.run(split("remove-maxProducers persistent://myprop/clust/ns1/ds2"));
        verify(mockTopics).removeMaxProducers("persistent://myprop/clust/ns1/ds2");

        cmdTopics.run(split("set-message-ttl persistent://myprop/clust/ns1/ds1 -t 30m"));
        verify(mockTopics).setMessageTTL("persistent://myprop/clust/ns1/ds1", 30 * 60);

        cmdTopics.run(split("get-message-ttl persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getMessageTTL("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("get-offload-policies persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getOffloadPolicies("persistent://myprop/clust/ns1/ds1", true);
        cmdTopics.run(split("get-max-unacked-messages-on-consumer persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getMaxUnackedMessagesOnConsumer("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("get-inactive-topic-policies persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getInactiveTopicPolicies("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("get-delayed-delivery persistent://myprop/clust/ns1/ds1 --applied"));
        verify(mockTopics).getDelayedDeliveryPolicy("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("get-max-consumers persistent://myprop/clust/ns1/ds1 -ap"));
        verify(mockTopics).getMaxConsumers("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("get-replication-clusters persistent://myprop/clust/ns1/ds1 --applied"));
        verify(mockTopics).getReplicationClusters("persistent://myprop/clust/ns1/ds1", true);

        cmdTopics.run(split("set-replication-clusters persistent://myprop/clust/ns1/ds1 -c test"));
        verify(mockTopics).setReplicationClusters("persistent://myprop/clust/ns1/ds1", Lists.newArrayList("test"));

        cmdTopics.run(split("remove-replication-clusters persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeReplicationClusters("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("get-shadow-topics persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getShadowTopics("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("set-shadow-topics persistent://myprop/clust/ns1/ds1 -t test"));
        verify(mockTopics).setShadowTopics("persistent://myprop/clust/ns1/ds1", Lists.newArrayList("test"));

        cmdTopics.run(split("remove-shadow-topics persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).removeShadowTopics("persistent://myprop/clust/ns1/ds1");

        cmdTopics.run(split("create-shadow-topic -s persistent://myprop/clust/ns1/source persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).createShadowTopic("persistent://myprop/clust/ns1/ds1", "persistent://myprop/clust/ns1/source", null);

        cmdTopics = new CmdTopics(() -> admin);
        cmdTopics.run(split("create-shadow-topic -p a=aa,b=bb,c=cc -s persistent://myprop/clust/ns1/source persistent://myprop/clust/ns1/ds1"));
        HashMap<String, String> p = new HashMap<>();
        p.put("a","aa");
        p.put("b","bb");
        p.put("c","cc");
        verify(mockTopics).createShadowTopic("persistent://myprop/clust/ns1/ds1", "persistent://myprop/clust/ns1/source", p);

        cmdTopics.run(split("get-shadow-source persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getShadowSource("persistent://myprop/clust/ns1/ds1");



    }

    private static LedgerInfo newLedger(long id, long entries, long size) {
        LedgerInfo l = new LedgerInfo();
        l.ledgerId = id;
        l.entries = entries;
        l.size = size;
        return l;
    }

    @Test
    public void persistentTopics() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Topics mockTopics = mock(Topics.class);
        when(admin.topics()).thenReturn(mockTopics);

        CmdPersistentTopics topics = new CmdPersistentTopics(() -> admin);

        topics.run(split("truncate persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).truncate("persistent://myprop/clust/ns1/ds1");

        topics.run(split("delete persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).delete("persistent://myprop/clust/ns1/ds1", false);

        topics.run(split("unload persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).unload("persistent://myprop/clust/ns1/ds1");

        topics.run(split("list myprop/clust/ns1"));
        verify(mockTopics).getList("myprop/clust/ns1");

        topics.run(split("subscriptions persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getSubscriptions("persistent://myprop/clust/ns1/ds1");

        topics.run(split("unsubscribe persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).deleteSubscription("persistent://myprop/clust/ns1/ds1", "sub1", false);

        topics.run(split("stats persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getStats("persistent://myprop/clust/ns1/ds1", false);

        topics.run(split("stats-internal persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getInternalStats("persistent://myprop/clust/ns1/ds1", false);

        topics.run(split("info-internal persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getInternalInfo("persistent://myprop/clust/ns1/ds1");

        topics.run(split("partitioned-stats persistent://myprop/clust/ns1/ds1 --per-partition"));
        verify(mockTopics).getPartitionedStats("persistent://myprop/clust/ns1/ds1", true);

        topics.run(split("partitioned-stats-internal persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getPartitionedInternalStats("persistent://myprop/clust/ns1/ds1");

        topics.run(split("skip-all persistent://myprop/clust/ns1/ds1 -s sub1"));
        verify(mockTopics).skipAllMessages("persistent://myprop/clust/ns1/ds1", "sub1");

        topics.run(split("skip persistent://myprop/clust/ns1/ds1 -s sub1 -n 100"));
        verify(mockTopics).skipMessages("persistent://myprop/clust/ns1/ds1", "sub1", 100);

        topics.run(split("expire-messages persistent://myprop/clust/ns1/ds1 -s sub1 -t 100"));
        verify(mockTopics).expireMessages("persistent://myprop/clust/ns1/ds1", "sub1", 100);

        topics.run(split("expire-messages-all-subscriptions persistent://myprop/clust/ns1/ds1 -t 100"));
        verify(mockTopics).expireMessagesForAllSubscriptions("persistent://myprop/clust/ns1/ds1", 100);

        topics.run(split("create-subscription persistent://myprop/clust/ns1/ds1 -s sub1 --messageId earliest -p a=b --property c=d -p x=y,z"));
        Map<String, String> props = new HashMap<>();
        props.put("a", "b");
        props.put("c", "d");
        props.put("x", "y,z");
        verify(mockTopics).createSubscription("persistent://myprop/clust/ns1/ds1", "sub1", MessageId.earliest, false, props);

        // jcommander is stateful, you cannot parse the same command twice
        topics = new CmdPersistentTopics(() -> admin);
        topics.run(split("create-subscription persistent://myprop/clust/ns1/ds1 -s sub1 --messageId earliest"));
        verify(mockTopics).createSubscription("persistent://myprop/clust/ns1/ds1", "sub1", MessageId.earliest, false, null);

        topics.run(split("create-partitioned-topic persistent://myprop/clust/ns1/ds1 --partitions 32"));
        verify(mockTopics).createPartitionedTopic("persistent://myprop/clust/ns1/ds1", 32);

        topics.run(split("list-partitioned-topics myprop/clust/ns1"));
        verify(mockTopics).getPartitionedTopicList("myprop/clust/ns1");

        topics.run(split("get-partitioned-topic-metadata persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).getPartitionedTopicMetadata("persistent://myprop/clust/ns1/ds1");

        topics.run(split("delete-partitioned-topic persistent://myprop/clust/ns1/ds1"));
        verify(mockTopics).deletePartitionedTopic("persistent://myprop/clust/ns1/ds1", false);

        topics.run(split("peek-messages persistent://myprop/clust/ns1/ds1 -s sub1 -n 3"));
        verify(mockTopics).peekMessages("persistent://myprop/clust/ns1/ds1", "sub1", 3);

        // cmd with option cannot be executed repeatedly
        topics = new CmdPersistentTopics(() -> admin);

        topics.run(split("expire-messages persistent://myprop/clust/ns1/ds1 -s sub1 -t 2h"));
        verify(mockTopics).expireMessages("persistent://myprop/clust/ns1/ds1", "sub1", 2 * 60 * 60);

        topics.run(split("expire-messages-all-subscriptions persistent://myprop/clust/ns1/ds1 -t 3d"));
        verify(mockTopics).expireMessagesForAllSubscriptions("persistent://myprop/clust/ns1/ds1", 3 * 60 * 60 * 24);

        // argument matcher for the timestamp in reset cursor. Since we can't verify exact timestamp, we check for a
        // range of +/- 1 second of the expected timestamp
        class TimestampMatcher implements ArgumentMatcher<Long> {
            @Override
            public boolean matches(Long timestamp) {
                long expectedTimestamp = System.currentTimeMillis() - (1 * 60 * 1000);
                if (timestamp < (expectedTimestamp + 1000) && timestamp > (expectedTimestamp - 1000)) {
                    return true;
                }
                return false;
            }
        }
        topics.run(split("reset-cursor persistent://myprop/clust/ns1/ds1 -s sub1 -t 1m"));
        verify(mockTopics).resetCursor(eq("persistent://myprop/clust/ns1/ds1"), eq("sub1"),
                longThat(new TimestampMatcher()));
    }

    @Test
    public void nonPersistentTopics() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Topics mockTopics = mock(Topics.class);
        when(admin.topics()).thenReturn(mockTopics);

        CmdTopics topics = new CmdTopics(() -> admin);

        topics.run(split("stats non-persistent://myprop/ns1/ds1"));
        verify(mockTopics).getStats("non-persistent://myprop/ns1/ds1", false, true, false);

        topics.run(split("stats-internal non-persistent://myprop/ns1/ds1"));
        verify(mockTopics).getInternalStats("non-persistent://myprop/ns1/ds1", false);

        topics.run(split("create-partitioned-topic non-persistent://myprop/ns1/ds1 --partitions 32"));
        verify(mockTopics).createPartitionedTopic("non-persistent://myprop/ns1/ds1", 32, null);

        topics.run(split("list myprop/ns1"));
        verify(mockTopics).getList("myprop/ns1", null, ListTopicsOptions.EMPTY);

        NonPersistentTopics mockNonPersistentTopics = mock(NonPersistentTopics.class);
        when(admin.nonPersistentTopics()).thenReturn(mockNonPersistentTopics);

        CmdNonPersistentTopics nonPersistentTopics = new CmdNonPersistentTopics(() -> admin);
        nonPersistentTopics.run(split("list-in-bundle myprop/clust/ns1 --bundle 0x23d70a30_0x26666658"));
        verify(mockNonPersistentTopics).getListInBundle("myprop/clust/ns1", "0x23d70a30_0x26666658");
    }

    @Test
    public void bookies() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Bookies mockBookies = mock(Bookies.class);
        doReturn(mockBookies).when(admin).bookies();
        doReturn(BookiesClusterInfo.builder().bookies(Collections.emptyList()).build()).when(mockBookies).getBookies();
        doReturn(new BookiesRackConfiguration()).when(mockBookies).getBookiesRackInfo();

        CmdBookies bookies = new CmdBookies(() -> admin);

        bookies.run(split("racks-placement"));
        verify(mockBookies).getBookiesRackInfo();

        bookies.run(split("list-bookies"));
        verify(mockBookies).getBookies();

        bookies.run(split("get-bookie-rack --bookie my-bookie:3181"));
        verify(mockBookies).getBookieRackInfo("my-bookie:3181");

        bookies.run(split("delete-bookie-rack --bookie my-bookie:3181"));
        verify(mockBookies).deleteBookieRackInfo("my-bookie:3181");

        bookies.run(split("set-bookie-rack --group my-group --bookie my-bookie:3181 --rack rack-1 --hostname host-1"));
        verify(mockBookies).updateBookieRackInfo("my-bookie:3181", "my-group",
                BookieInfo.builder()
                        .rack("rack-1")
                        .hostname("host-1")
                        .build());

        // test invalid rack name ""
        try {
            BookieInfo.builder().rack("").hostname("host-1").build();
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "rack name is invalid, it should not be null, empty or '/'");
        }

        // test invalid rack name "/"
        try {
            BookieInfo.builder().rack("/").hostname("host-1").build();
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "rack name is invalid, it should not be null, empty or '/'");
        }

    }

    @Test
    public void requestTimeout() throws Exception {
        Properties properties = new Properties();
        properties.put("webServiceUrl", "http://localhost:2181");
        PulsarAdminTool tool = new PulsarAdminTool(properties);

        try {
            tool.run("--request-timeout 1".split(" "));
        } catch (Exception e) {
            //Ok
        }

        @Cleanup
        PulsarAdminImpl pulsarAdmin = (PulsarAdminImpl) tool.getPulsarAdminSupplier().get();
        ClientConfigurationData conf =  pulsarAdmin.getClientConfigData();

        assertEquals(1000, conf.getRequestTimeoutMs());
    }

    @Test
    public void testSourceCreateMissingSourceConfigFileFaileWithExitCode1() throws Exception {
        Properties properties = new Properties();
        properties.put("webServiceUrl", "http://localhost:2181");
        PulsarAdminTool tool = new PulsarAdminTool(properties);
        assertFalse(tool.run("sources create --source-config-file doesnotexist.yaml".split(" ")));
    }

    @Test
    public void testSourceUpdateMissingSourceConfigFileFaileWithExitCode1() throws Exception {
        Properties properties = new Properties();
        properties.put("webServiceUrl", "http://localhost:2181");
        PulsarAdminTool tool = new PulsarAdminTool(properties);

        assertFalse(tool.run("sources update --source-config-file doesnotexist.yaml".split(" ")));
    }

    @Test
    public void testAuthTlsWithJsonParam() throws Exception {

        Properties properties = new Properties();
        properties.put("authPlugin", AuthenticationTls.class.getName());
        Map<String, String> paramMap = new HashMap<>();
        final String certFilePath = "/my-file:role=name.cert";
        final String keyFilePath = "/my-file:role=name.key";
        paramMap.put("tlsCertFile", certFilePath);
        paramMap.put("tlsKeyFile", keyFilePath);
        final String paramStr = ObjectMapperFactory.getMapper().writer().writeValueAsString(paramMap);
        properties.put("authParams", paramStr);
        properties.put("webServiceUrl", "http://localhost:2181");
        PulsarAdminTool tool = new PulsarAdminTool(properties);
        try {
            tool.run("brokers list use".split(" "));
        } catch (Exception e) {
            // Ok
        }

        // validate Authentication-tls has been configured
        @Cleanup
        PulsarAdminImpl pulsarAdmin = (PulsarAdminImpl) tool.getPulsarAdminSupplier().get();
        ClientConfigurationData conf =  pulsarAdmin.getClientConfigData();
        AuthenticationTls atuh = (AuthenticationTls) conf.getAuthentication();
        assertEquals(atuh.getCertFilePath(), certFilePath);
        assertEquals(atuh.getKeyFilePath(), keyFilePath);

        properties.put("authParams", String.format("tlsCertFile:%s,tlsKeyFile:%s", certFilePath, keyFilePath));
        tool = new PulsarAdminTool(properties);
        try {
            tool.run("brokers list use".split(" "));
        } catch (Exception e) {
            // Ok
        }

        @Cleanup
        PulsarAdminImpl pulsarAdmin2 = (PulsarAdminImpl) tool.getPulsarAdminSupplier().get();
        conf =  pulsarAdmin2.getClientConfigData();
        atuh = (AuthenticationTls) conf.getAuthentication();
        assertEquals(atuh.getCertFilePath(), certFilePath);
        assertEquals(atuh.getKeyFilePath(), keyFilePath);
    }

    @Test
    void proxy() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        ProxyStats mockProxyStats = mock(ProxyStats.class);
        doReturn(mockProxyStats).when(admin).proxyStats();

        CmdProxyStats proxyStats = new CmdProxyStats(() -> admin);

        proxyStats.run(split("connections"));
        verify(mockProxyStats).getConnections();

        proxyStats.run(split("topics"));
        verify(mockProxyStats).getTopics();
    }

    @Test
    void transactions() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Transactions transactions = Mockito.mock(Transactions.class);
        doReturn(transactions).when(admin).transactions();

        CmdTransactions cmdTransactions = new CmdTransactions(() -> admin);

        cmdTransactions.run(split("coordinator-stats -c 1"));
        verify(transactions).getCoordinatorStatsById(1);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("coordinator-stats"));
        verify(transactions).getCoordinatorStats();

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("coordinator-internal-stats -c 1 -m"));
        verify(transactions).getCoordinatorInternalStats(1, true);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("transaction-in-buffer-stats -m 1 -t test -l 2"));
        verify(transactions).getTransactionInBufferStats(new TxnID(1, 2), "test");

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("transaction-in-pending-ack-stats -m 1 -l 2 -t test -s test"));
        verify(transactions).getTransactionInPendingAckStats(
                new TxnID(1, 2), "test", "test");

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("transaction-metadata -m 1 -l 2"));
        verify(transactions).getTransactionMetadata(new TxnID(1, 2));

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("slow-transactions -c 1 -t 1h"));
        verify(transactions).getSlowTransactionsByCoordinatorId(
                1, 3600000, TimeUnit.MILLISECONDS);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("slow-transactions -t 1h"));
        verify(transactions).getSlowTransactions(3600000, TimeUnit.MILLISECONDS);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("transaction-buffer-stats -t test -l"));
        verify(transactions).getTransactionBufferStats("test", true, false);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("pending-ack-stats -t test -s test -l"));
        verify(transactions).getPendingAckStats("test", "test", true);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("pending-ack-internal-stats -t test -s test"));
        verify(transactions).getPendingAckInternalStats("test", "test", false);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("buffer-snapshot-internal-stats -t test"));
        verify(transactions).getTransactionBufferInternalStats("test", false);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("scale-transactionCoordinators -r 3"));
        verify(transactions).scaleTransactionCoordinators(3);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("position-stats-in-pending-ack -t test -s test -l 1 -e 1 -b 1"));
        verify(transactions).getPositionStatsInPendingAck("test", "test", 1L, 1L, 1);

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("coordinators-list"));
        verify(transactions).listTransactionCoordinators();

        cmdTransactions = new CmdTransactions(() -> admin);
        cmdTransactions.run(split("abort-transaction -m 1 -l 2"));
        verify(transactions).abortTransaction(new TxnID(1, 2));
    }

    @Test
    void schemas() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Schemas schemas = Mockito.mock(Schemas.class);
        doReturn(schemas).when(admin).schemas();

        CmdSchemas cmdSchemas = new CmdSchemas(() -> admin);
        cmdSchemas.run(split("get -v 1 persistent://tn1/ns1/tp1"));
        verify(schemas).getSchemaInfo("persistent://tn1/ns1/tp1", 1);

        cmdSchemas = new CmdSchemas(() -> admin);
        cmdSchemas.run(split("get -a persistent://tn1/ns1/tp1"));
        verify(schemas).getAllSchemas("persistent://tn1/ns1/tp1");

        cmdSchemas = new CmdSchemas(() -> admin);
        cmdSchemas.run(split("get persistent://tn1/ns1/tp1"));
        verify(schemas).getSchemaInfoWithVersion("persistent://tn1/ns1/tp1");

        cmdSchemas = new CmdSchemas(() -> admin);
        cmdSchemas.run(split("delete persistent://tn1/ns1/tp1"));
        verify(schemas).deleteSchema("persistent://tn1/ns1/tp1", false);

        cmdSchemas = new CmdSchemas(() -> admin);
        cmdSchemas.run(split("delete persistent://tn1/ns1/tp1 -f"));
        verify(schemas).deleteSchema("persistent://tn1/ns1/tp1", true);

        cmdSchemas = new CmdSchemas(() -> admin);
        String schemaFile = PulsarAdminToolTest.class.getClassLoader()
                .getResource("test_schema_create.json").getFile();
        cmdSchemas.run(split("upload -f " + schemaFile + " persistent://tn1/ns1/tp1"));
        PostSchemaPayload input = new ObjectMapper().readValue(new File(schemaFile), PostSchemaPayload.class);
        verify(schemas).createSchema("persistent://tn1/ns1/tp1", input);

        cmdSchemas = new CmdSchemas(() -> admin);
        cmdSchemas.run(split("compatibility -f " + schemaFile + " persistent://tn1/ns1/tp1"));
        input = new ObjectMapper().readValue(new File(schemaFile), PostSchemaPayload.class);
        verify(schemas).testCompatibility("persistent://tn1/ns1/tp1", input);

        cmdSchemas = new CmdSchemas(() -> admin);
        String jarFile = PulsarAdminToolTest.class.getClassLoader()
                .getResource("dummyexamples.jar").getFile();
        String className = SchemaDemo.class.getName();
        cmdSchemas.run(split("extract -j " + jarFile + " -c " + className + " -t json persistent://tn1/ns1/tp1"));
        File file = new File(jarFile);
        ClassLoader cl = new URLClassLoader(new URL[]{file.toURI().toURL()});
        Class cls = cl.loadClass(className);
        SchemaDefinition<Object> schemaDefinition =
                SchemaDefinition.builder()
                        .withPojo(cls)
                        .withAlwaysAllowNull(true)
                        .build();
        PostSchemaPayload postSchemaPayload = new PostSchemaPayload();
        postSchemaPayload.setType("JSON");
        postSchemaPayload.setSchema(SchemaExtractor.getJsonSchemaInfo(schemaDefinition));
        postSchemaPayload.setProperties(schemaDefinition.getProperties());
        verify(schemas).createSchema("persistent://tn1/ns1/tp1", postSchemaPayload);
    }

    @Test
    public void customCommands() throws Exception {

        // see the custom command help in the main help
        String logs = runCustomCommand(new String[]{"-h"});
        assertTrue(logs.contains("customgroup"));
        assertTrue(logs.contains("Custom group 1 description"));

        // missing subcommand
        logs = runCustomCommand(new String[]{"customgroup"});
        assertTrue(logs.contains("Missing required subcommand"));
        assertTrue(logs.contains("Command 1 description"));
        assertTrue(logs.contains("Command 2 description"));

        // missing required parameter
        logs = runCustomCommand(new String[]{"customgroup", "command1"});
        assertTrue(logs.contains("Missing required options and parameters"));
        assertTrue(logs.contains("Command 1 description"));

        logs = runCustomCommand(new String[]{"customgroup", "command1", "mytopic"});
        assertTrue(logs.contains("Command 1 description"));
        assertTrue(logs.contains("Missing required option"));

        // run a comand that uses PulsarAdmin API
        logs = runCustomCommand(new String[]{"customgroup", "command1", "--type", "stats", "mytopic"});
        assertTrue(logs.contains("Execute:"));
        // parameters
        assertTrue(logs.contains("--type=stats"));
        // configuration
        assertTrue(logs.contains("webServiceUrl=http://localhost:2181"));
        // execution of the PulsarAdmin command
        assertTrue(logs.contains("Topic stats: MOCK-TOPIC-STATS"));


        // run a command that uses all parameter types
        logs = runCustomCommand(new String[]{"customgroup", "command2",
                "-s", "mystring",
                "-i", "123",
                "-b", "true", // boolean variable, true|false
                "-bf", // boolean flag, no arguments
                "mainParameterValue"});
        assertTrue(logs.contains("Execute:"));
        // parameters
        assertTrue(logs.contains("-s=mystring"));
        assertTrue(logs.contains("-i=123"));
        assertTrue(logs.contains("-b=true"));
        assertTrue(logs.contains("-bf=true")); // boolean flag, passed = true
        assertTrue(logs.contains("main=mainParameterValue"));


        // run a command that uses all parameter types, see the default value
        logs = runCustomCommand(new String[]{"customgroup", "command2"});
        assertTrue(logs.contains("Execute:"));
        // parameters
        assertTrue(logs.contains("-s=null"));
        assertTrue(logs.contains("-i=0"));
        assertTrue(logs.contains("-b=null"));
        assertTrue(logs.contains("-bf=false")); // boolean flag, not passed = false
        assertTrue(logs.contains("main=null"));

    }

    @Test
    public void customCommandsFactoryImmutable() throws Exception {
        File narFile = new File(PulsarAdminTool.class.getClassLoader()
                .getResource("cliextensions/customCommands-nar.nar").getFile());
        log.info("NAR FILE is {}", narFile);

        PulsarAdminBuilder builder = mock(PulsarAdminBuilder.class);
        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(builder.build()).thenReturn(admin);
        Topics topics = mock(Topics.class);
        when(admin.topics()).thenReturn(topics);
        TopicStats topicStats = mock(TopicStats.class);
        when(topics.getStats(anyString())).thenReturn(topicStats);
        when(topicStats.toString()).thenReturn("MOCK-TOPIC-STATS");

        Properties properties = new Properties();
        properties.put("webServiceUrl", "http://localhost:2181");
        properties.put("cliExtensionsDirectory", narFile.getParentFile().getAbsolutePath());
        properties.put("customCommandFactories", "dummy");
        PulsarAdminTool tool = new PulsarAdminTool(properties);
        List<CustomCommandFactory> customCommandFactories = tool.customCommandFactories;
        assertNotNull(customCommandFactories);
        tool.run(split("-h"));
        assertSame(tool.customCommandFactories, customCommandFactories);
    }

    @Test
    public void testHelpFlag() throws Exception {
        Properties properties = new Properties();
        properties.put("webServiceUrl", "http://localhost:8080");

        PulsarAdminTool pulsarAdminTool = new PulsarAdminTool(properties);

        {
            assertTrue(pulsarAdminTool.run(split("schemas -h")));
        }

        {
            assertTrue(pulsarAdminTool.run(split("schemas --help")));
        }

        {
            assertTrue(pulsarAdminTool.run(split("schemas delete -h")));
        }

        {
            assertTrue(pulsarAdminTool.run(split("schemas delete --help")));
        }
    }

    private static String runCustomCommand(String[] args) throws Exception {
        File narFile = new File(PulsarAdminTool.class.getClassLoader()
                .getResource("cliextensions/customCommands-nar.nar").getFile());
        log.info("NAR FILE is {}", narFile);

        PulsarAdminBuilder builder = mock(PulsarAdminBuilder.class);
        PulsarAdmin admin = mock(PulsarAdmin.class);
        when(builder.build()).thenReturn(admin);
        Topics topics = mock(Topics.class);
        when(admin.topics()).thenReturn(topics);
        TopicStats topicStats = mock(TopicStats.class);
        when(topics.getStats(anyString())).thenReturn(topicStats);
        when(topicStats.toString()).thenReturn("MOCK-TOPIC-STATS");

        Properties properties = new Properties();
        properties.put("webServiceUrl", "http://localhost:2181");
        properties.put("cliExtensionsDirectory", narFile.getParentFile().getAbsolutePath());
        properties.put("customCommandFactories", "dummy");
        PulsarAdminTool tool = new PulsarAdminTool(properties);
        tool.getPulsarAdminSupplier().setAdminBuilder(builder);
        StringBuilder logs = new StringBuilder();
        try (CaptureStdOut capture = new CaptureStdOut(tool.commander, logs)) {
            tool.run(args);
        }
        log.info("Captured out: {}", logs);
        return logs.toString();
    }

    private static class CaptureStdOut implements AutoCloseable {
        final PrintStream currentOut = System.out;
        final PrintStream currentErr = System.err;
        final ByteArrayOutputStream logs;
        final PrintStream capturedOut;
        final StringBuilder receiver;
        public CaptureStdOut(CommandLine commandLine, StringBuilder receiver) {
            logs = new ByteArrayOutputStream();
            capturedOut = new PrintStream(logs, true);
            this.receiver = receiver;
            PrintWriter printWriter = new PrintWriter(logs);
            commandLine.setErr(printWriter);
            commandLine.setOut(printWriter);
            System.setOut(new PrintStream(logs));
            System.setErr(new PrintStream(logs));
        }
        public void close() {
            capturedOut.flush();
            System.setOut(currentOut);
            System.setErr(currentErr);
            receiver.append(logs.toString(StandardCharsets.UTF_8));
        }
    }

    public static class SchemaDemo {
        public SchemaDemo() {
        }

        public static void main(String[] args) {
        }
    }

    String[] split(String s) {
        return s.split(" ");
    }
}
