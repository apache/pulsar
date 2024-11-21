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
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableViewImpl;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

public abstract class ExtensibleLoadManagerImplBaseTest extends MockedPulsarServiceBaseTest {

    final static String caCertPath = Resources.getResource("certificate-authority/certs/ca.cert.pem").getPath();
    final static String brokerCertPath =
            Resources.getResource("certificate-authority/server-keys/broker.cert.pem").getPath();
    final static String brokerKeyPath =
            Resources.getResource("certificate-authority/server-keys/broker.key-pk8.pem").getPath();

    protected PulsarService pulsar1;
    protected PulsarService pulsar2;

    protected PulsarTestContext additionalPulsarTestContext;

    protected ExtensibleLoadManagerImpl primaryLoadManager;

    protected ExtensibleLoadManagerImpl secondaryLoadManager;

    protected ServiceUnitStateChannelImpl channel1;
    protected ServiceUnitStateChannelImpl channel2;

    protected final String defaultTestNamespace;

    protected LookupService lookupService;

    protected String serviceUnitStateTableViewClassName;

    protected ArrayList<PulsarClient> clients = new ArrayList<>();

    @DataProvider(name = "serviceUnitStateTableViewClassName")
    public static Object[][] serviceUnitStateTableViewClassName() {
        return new Object[][]{
                {ServiceUnitStateTableViewImpl.class.getName()},
                {ServiceUnitStateMetadataStoreTableViewImpl.class.getName()}
        };
    }

    protected ExtensibleLoadManagerImplBaseTest(String defaultTestNamespace, String serviceUnitStateTableViewClassName) {
        this.defaultTestNamespace = defaultTestNamespace;
        this.serviceUnitStateTableViewClassName = serviceUnitStateTableViewClassName;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        updateConfig(conf);
    }


    protected ServiceConfiguration updateConfig(ServiceConfiguration conf) {
        conf.setForceDeleteNamespaceAllowed(true);
        conf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        conf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        conf.setLoadManagerServiceUnitStateTableViewClassName(serviceUnitStateTableViewClassName);
        conf.setLoadBalancerReportUpdateMaxIntervalMinutes(1);
        conf.setLoadBalancerSheddingEnabled(false);
        conf.setLoadBalancerDebugModeEnabled(true);
        conf.setWebServicePortTls(Optional.of(0));
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(brokerCertPath);
        conf.setTlsKeyFilePath(brokerKeyPath);
        conf.setTlsTrustCertsFilePath(caCertPath);
        return conf;
    }

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        super.internalSetup(conf);
        pulsar1 = pulsar;
        var conf2 = updateConfig(getDefaultConf());
        additionalPulsarTestContext = createAdditionalPulsarTestContext(conf2);
        pulsar2 = additionalPulsarTestContext.getPulsarService();

        setPrimaryLoadManager();
        setSecondaryLoadManager();

        admin.clusters().createCluster(this.conf.getClusterName(),
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"),
                        Sets.newHashSet(this.conf.getClusterName())));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default",
                Sets.newHashSet(this.conf.getClusterName()));

        admin.namespaces().createNamespace(defaultTestNamespace, 128);
        admin.namespaces().setNamespaceReplicationClusters(defaultTestNamespace,
                Sets.newHashSet(this.conf.getClusterName()));
        lookupService = (LookupService) FieldUtils.readDeclaredField(pulsarClient, "lookup", true);

        for (int i = 0; i < 4; i++) {
            clients.add(pulsarClient(lookupUrl.toString(), 100));
        }
    }

    private static PulsarClient pulsarClient(String url, int intervalInMillis) throws PulsarClientException {
        return
                PulsarClient.builder()
                        .serviceUrl(url)
                        .statsInterval(intervalInMillis, TimeUnit.MILLISECONDS).build();
    }


    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (PulsarClient client : clients) {
            futures.add(client.closeAsync());
        }
        futures.add(pulsar2.closeAsync());

        if (additionalPulsarTestContext != null) {
            additionalPulsarTestContext.close();
            additionalPulsarTestContext = null;
        }
        super.internalCleanup();
        try {
            FutureUtil.waitForAll(futures).join();
        } catch (Throwable e) {
            // skip error
        }
        pulsar1 = pulsar2 = null;
        primaryLoadManager = secondaryLoadManager = null;
        channel1 = channel2 = null;
        lookupService = null;

    }

    @BeforeMethod(alwaysRun = true)
    protected void initializeState() throws PulsarAdminException, IllegalAccessException {
        admin.namespaces().unload(defaultTestNamespace);
        reset(primaryLoadManager, secondaryLoadManager);
        FieldUtils.writeDeclaredField(pulsarClient, "lookup", lookupService, true);
        pulsar1.getConfig().setLoadBalancerMultiPhaseBundleUnload(true);
        pulsar2.getConfig().setLoadBalancerMultiPhaseBundleUnload(true);
    }

    protected void setPrimaryLoadManager() throws IllegalAccessException {
        ExtensibleLoadManagerWrapper wrapper =
                (ExtensibleLoadManagerWrapper) pulsar1.getLoadManager().get();
        primaryLoadManager = spy((ExtensibleLoadManagerImpl)
                FieldUtils.readField(wrapper, "loadManager", true));
        FieldUtils.writeField(wrapper, "loadManager", primaryLoadManager, true);
        channel1 = (ServiceUnitStateChannelImpl)
                FieldUtils.readField(primaryLoadManager, "serviceUnitStateChannel", true);
    }

    private void setSecondaryLoadManager() throws IllegalAccessException {
        ExtensibleLoadManagerWrapper wrapper =
                (ExtensibleLoadManagerWrapper) pulsar2.getLoadManager().get();
        secondaryLoadManager = spy((ExtensibleLoadManagerImpl)
                FieldUtils.readField(wrapper, "loadManager", true));
        FieldUtils.writeField(wrapper, "loadManager", secondaryLoadManager, true);
        channel2 = (ServiceUnitStateChannelImpl)
                FieldUtils.readField(secondaryLoadManager, "serviceUnitStateChannel", true);
    }

    protected static CompletableFuture<NamespaceBundle> getBundleAsync(PulsarService pulsar, TopicName topic) {
        return pulsar.getNamespaceService().getBundleAsync(topic);
    }

    protected Pair<TopicName, NamespaceBundle> getBundleIsNotOwnByChangeEventTopic(String topicNamePrefix)
            throws Exception {
        TopicName changeEventsTopicName =
                TopicName.get(defaultTestNamespace + "/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
        NamespaceBundle changeEventsBundle = getBundleAsync(pulsar1, changeEventsTopicName).get();
        int i = 0;
        while(true) {
            TopicName topicName = TopicName.get(defaultTestNamespace + "/" + topicNamePrefix + "-" + i);
            NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();
            if (!bundle.equals(changeEventsBundle)) {
                return Pair.of(topicName, bundle);
            }
            i++;
        }
    }
}
