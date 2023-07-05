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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.collect.Sets;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.ListNamespaceTopicsOptions;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicType;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class PulsarAdminToolConverterTest {

    @Test
    public void namespacesSimpl() throws Exception {
        PulsarAdmin admin = Mockito.mock(PulsarAdmin.class);
        Namespaces mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        Lookup mockLookup = mock(Lookup.class);
        when(admin.lookups()).thenReturn(mockLookup);

        CmdNamespaces namespaces = new CmdNamespaces(() -> admin);

        mockNamespaces = mock(Namespaces.class);
        when(admin.namespaces()).thenReturn(mockNamespaces);
        namespaces = new CmdNamespaces(() -> admin);

        namespaces.run(split(
                "set-offload-policies myprop/clust/ns1 -r test-region -d aws-s3 -b test-bucket -e http://test.endpoint -mbs 32M -rbs 5M -oat 10M -oats 100 -oae 10s -orp tiered-storage-first"));
        verify(mockNamespaces).setOffloadPolicies("myprop/clust/ns1",
                OffloadPoliciesImpl.create("aws-s3", "test-region", "test-bucket",
                        "http://test.endpoint", null, null, null, null, 32 * 1024 * 1024, 5 * 1024 * 1024,
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

        namespaces.run(split("set-delayed-delivery myprop/clust/ns1 -e -t 1s"));
        verify(mockNamespaces).setDelayedDeliveryMessages("myprop/clust/ns1",
                DelayedDeliveryPolicies.builder().tickTime(1000).active(true).build());

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
                        "http://test.endpoint", null, null, null, null, 32 * 1024 * 1024, 5 * 1024 * 1024,
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

    }

    String[] split(String s) {
        return s.split(" ");
    }
}
