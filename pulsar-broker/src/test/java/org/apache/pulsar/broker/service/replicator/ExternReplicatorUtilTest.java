/**
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
package org.apache.pulsar.broker.service.replicator;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies.ReplicatorType;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.ReplicatorPolicies;
import org.apache.pulsar.common.policies.data.ReplicatorPoliciesRequest;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.functions.worker.FunctionMetaDataManager;
import org.apache.pulsar.functions.worker.WorkerService;
import org.apache.pulsar.replicator.api.AbstractReplicatorManager;
import org.apache.pulsar.replicator.api.kinesis.KinesisReplicatorProvider;
import org.apache.pulsar.replicator.auth.DefaultAuthParamKeyStore;
import org.apache.pulsar.replicator.function.ReplicatorFunction;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import jersey.repackaged.com.google.common.collect.Lists;

/**
 * Test {@link ExternReplicatorUtil} functionality
 *
 */
public class ExternReplicatorUtilTest extends ProducerConsumerBase {
    LocalBookkeeperEnsemble bkEnsemble;

    final String tenant = "external-repl-prop";
    String pulsarFunctionsNamespace = tenant + "/use/pulsar-function-admin";
    final String namespacePortion = "myReplNs";
    final String replNamespace = tenant + "/" + namespacePortion;
    final String topicName = "pulsarTopic";
    final String regionName = "us-east";
    final String kinesisReplicatorTopic = "persistent://" + replNamespace + "/" + topicName;
    final ReplicatorType replicatorType = ReplicatorType.Kinesis;

    final String inputTopic = "persistent://" + tenant + "/use/ns/input";
    final String outputTopic = "persistent://" + tenant + "/use/ns/output";

    private static final Logger log = LoggerFactory.getLogger(ExternReplicatorUtilTest.class);

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
        admin.tenants().createTenant(tenant,
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAddReplicatorPoliciesToNamespace() throws Exception {
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("test"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        ReplicatorPoliciesRequest replicatorPolicieRequest = createReplicatorPoliciesRequest();

        // add replicator policies for a namespace
        admin.namespaces().addExternalReplicator(replNamespace, ReplicatorType.Kinesis, regionName,
                replicatorPolicieRequest);
        retryStrategically((test) -> {
            Map<ReplicatorType, Map<String, ReplicatorPolicies>> policies;
            try {
                policies = admin.namespaces().getPolicies(replNamespace).replicatorPolicies;
            } catch (PulsarAdminException e) {
                return false;
            }
            return policies != null && policies.size() > 0;
        }, 5, 100);
        // check replicator policies
        ReplicatorPolicies policeis = admin.namespaces().getPolicies(replNamespace).replicatorPolicies
                .get(ReplicatorType.Kinesis).get(regionName);
        Assert.assertEquals(policeis, replicatorPolicieRequest.replicatorPolicies);

        // remove replicator policies for a namespace
        admin.namespaces().removeExternalReplicator(replNamespace, ReplicatorType.Kinesis, regionName);
        retryStrategically((test) -> {
            Map<ReplicatorType, Map<String, ReplicatorPolicies>> policies;
            try {
                policies = admin.namespaces().getPolicies(replNamespace).replicatorPolicies;
            } catch (PulsarAdminException e) {
                return false;
            }
            return policies == null || policies.size() == 0;
        }, 5, 100);
        // check replicator policies
        policeis = admin.namespaces().getPolicies(replNamespace).replicatorPolicies.get(ReplicatorType.Kinesis)
                .get(regionName);
        Assert.assertEquals(policeis, null);
    }

    /**
     * Test add replicator topic without adding replicator-stream-name mapping
     */
    @Test
    public void testInvalidTopic() throws Exception {
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("test"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        ReplicatorPoliciesRequest replicatorPolicieRequest = createReplicatorPoliciesRequest();
        // add replicator policies for a namespace
        admin.namespaces().addExternalReplicator(replNamespace, ReplicatorType.Kinesis, regionName,
                replicatorPolicieRequest);
        try {
            admin.persistentTopics().registerReplicator(kinesisReplicatorTopic + "invalid", replicatorType, regionName);
            Assert.fail("should have failed for missing topic-name mapping");
        } catch (org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException e) {
            // Ok. Topic mapping not found
        }
    }

    @Test
    public void testRegisterReplicator() throws Exception {
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("test"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        ReplicatorPoliciesRequest replicatorPolicieRequest = createReplicatorPoliciesRequest();
        TopicName clientReplTopicName = TopicName.get(kinesisReplicatorTopic);
        // add replicator policies for a namespace
        admin.namespaces().addExternalReplicator(replNamespace, ReplicatorType.Kinesis, regionName,
                replicatorPolicieRequest);
        WorkerService worker = mock(WorkerService.class);
        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        when(worker.getFunctionMetaDataManager()).thenReturn(functionMetaDataManager);
        String functionName = ExternReplicatorUtil.formFunctionName(replicatorType, clientReplTopicName);
        // mock: replicator function is already registered: so, expecting function already exist
        when(functionMetaDataManager.containsFunction(ReplicatorFunction.CONF_REPLICATOR_TENANT_VAL,
                ReplicatorFunction.CONF_REPLICATOR_NAMESPACE_VAL, functionName)).thenReturn(true);
        doReturn(worker).when(pulsar).getWorkerService();
        Response response = ExternReplicatorUtil.internalRegisterReplicatorOnTopic(pulsar, clientReplTopicName,
                replicatorType, regionName);
        Assert.assertEquals(response.getStatus(), Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testDeregisterReplicator() throws Exception {
        pulsar.getConfiguration().setClusterName("test");
        admin.namespaces().createNamespace(replNamespace);
        Set<String> clusters = Sets.newHashSet(Lists.newArrayList("test"));
        admin.namespaces().setNamespaceReplicationClusters(replNamespace, clusters);
        ReplicatorPoliciesRequest replicatorPolicieRequest = createReplicatorPoliciesRequest();
        ExternReplicatorUtil.createNamespaceIfNotCreated(pulsar,
                TopicName.get(ReplicatorFunction.getFunctionTopicName(replicatorType, namespacePortion)));
        TopicName clientReplTopicName = TopicName.get(kinesisReplicatorTopic);
        // add replicator policies for a namespace
        admin.namespaces().addExternalReplicator(replNamespace, ReplicatorType.Kinesis, regionName,
                replicatorPolicieRequest);
        WorkerService worker = mock(WorkerService.class);
        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        when(worker.getFunctionMetaDataManager()).thenReturn(functionMetaDataManager);
        when(functionMetaDataManager.containsFunction(any(), any(), any())).thenReturn(false);
        doReturn(worker).when(pulsar).getWorkerService();
        BrokerService broker = spy(pulsar.getBrokerService());
        doReturn(pulsarClient).when(broker).getReplicationClient(any());
        doReturn(broker).when(pulsar).getBrokerService();
        // create a subscription that replicator-function is going to create
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(kinesisReplicatorTopic)
                .subscriptionName(AbstractReplicatorManager.formSubscriptionName(replicatorType, regionName))
                .subscribe();
        consumer.close();
        retryStrategically((test) -> {
            try {
                return admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions.size() == 1;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        Map<String, SubscriptionStats> subscription = admin.persistentTopics()
                .getStats(kinesisReplicatorTopic).subscriptions;
        Assert.assertEquals(subscription.size(), 1);
        // deregister function that deletes the subscription of function and try to delete function
        Response response = ExternReplicatorUtil.internalDeregisterReplicatorOnTopic(pulsar, clientReplTopicName,
                replicatorType, regionName);
        retryStrategically((test) -> {
            try {
                return admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions.size() == 0;
            } catch (PulsarAdminException e) {
                return false;
            }
        }, 5, 150);
        subscription = admin.persistentTopics().getStats(kinesisReplicatorTopic).subscriptions;
        Assert.assertEquals(subscription.size(), 0);
        Assert.assertEquals(response.getStatus(), Status.NOT_FOUND.getStatusCode());
    }

    private ReplicatorPoliciesRequest createReplicatorPoliciesRequest() {
        ReplicatorPolicies replicatorPolicies = new ReplicatorPolicies();
        Map<String, String> topicMapping = Maps.newHashMap();
        Map<String, String> replicationProperties = Maps.newHashMap();
        Map<String, String> authParamData = Maps.newHashMap();
        replicationProperties.put(KinesisReplicatorProvider.ACCESS_KEY_NAME, "ak");
        replicationProperties.put(KinesisReplicatorProvider.SECRET_KEY_NAME, "sk");
        topicMapping.put(topicName, "KineisFunction:us-west-2");
        replicatorPolicies.topicNameMapping = topicMapping;
        replicatorPolicies.replicationProperties = replicationProperties;
        replicatorPolicies.authParamStorePluginName = DefaultAuthParamKeyStore.class.getName();
        ReplicatorPoliciesRequest replicatorPolicieRequest = new ReplicatorPoliciesRequest();
        replicatorPolicieRequest.replicatorPolicies = replicatorPolicies;
        replicatorPolicieRequest.authParamData = authParamData;
        return replicatorPolicieRequest;
    }

}
