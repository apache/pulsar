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

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.loadbalance.AntiAffinityNamespaceGroupTest;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.filter.AntiAffinityGroupPolicyFilter;
import org.apache.pulsar.broker.loadbalance.extensions.policies.AntiAffinityGroupPolicyHelper;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AntiAffinityNamespaceGroupExtensionTest extends AntiAffinityNamespaceGroupTest {

    final String bundle = "0x00000000_0xffffffff";
    final String nsSuffix = "-antiaffinity-enabled";

    protected Object getBundleOwnershipData() {
        return new HashSet<Map.Entry<String, ServiceUnitStateData>>();
    }

    protected String getLoadManagerClassName() {
        return ExtensibleLoadManagerImpl.class.getName();
    }

    protected String selectBroker(ServiceUnitId serviceUnit, Object loadManager) {
        try {
            return ((ExtensibleLoadManagerImpl) loadManager)
                    .assign(Optional.empty(), serviceUnit, LookupOptions.builder().build()).get()
                    .get().getPulsarServiceUrl();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    protected void selectBrokerForNamespace(
            Object ownershipData,
            String broker, String namespace, String assignedBundleName) {

        Set<Map.Entry<String, ServiceUnitStateData>> ownershipDataSet =
                (Set<Map.Entry<String, ServiceUnitStateData>>) ownershipData;
        ownershipDataSet.add(
                new AbstractMap.SimpleEntry<String, ServiceUnitStateData>(
                        assignedBundleName,
                        new ServiceUnitStateData(Owned, broker, 1)));

    }

    protected void verifyLoadSheddingWithAntiAffinityNamespace(String namespace, String bundle) {
        // No-op
    }

    protected boolean isLoadManagerUpdatedDomainCache(Object loadManager) throws Exception {
        @SuppressWarnings("unchecked")
        var antiAffinityGroupPolicyHelper =
                (AntiAffinityGroupPolicyHelper)
                        FieldUtils.readDeclaredField(
                                loadManager, "antiAffinityGroupPolicyHelper", true);
        var brokerToFailureDomainMap = (Map<String, String>)
                org.apache.commons.lang.reflect.FieldUtils.readDeclaredField(antiAffinityGroupPolicyHelper,
                        "brokerToFailureDomainMap", true);
        return !brokerToFailureDomainMap.isEmpty();
    }

    @Test
    public void testAntiAffinityGroupPolicyFilter()
            throws IllegalAccessException, ExecutionException, InterruptedException,
            TimeoutException, PulsarAdminException, PulsarClientException {

        final String namespace = "my-tenant/test/my-ns-filter";
        final String namespaceAntiAffinityGroup = "my-antiaffinity-filter";


        final String antiAffinityEnabledNameSpace = namespace + nsSuffix;
        admin.namespaces().createNamespace(antiAffinityEnabledNameSpace);
        admin.namespaces().setNamespaceAntiAffinityGroup(antiAffinityEnabledNameSpace, namespaceAntiAffinityGroup);
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsar.getSafeWebServiceAddress()).build();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(
                        "persistent://" + antiAffinityEnabledNameSpace + "/my-topic1")
                .create();
        pulsar.getBrokerService().updateRates();
        var brokerRegistry =
                (BrokerRegistry)
                        FieldUtils.readDeclaredField(
                                primaryLoadManager, "brokerRegistry", true);
        var antiAffinityGroupPolicyFilter =
                (AntiAffinityGroupPolicyFilter)
                        FieldUtils.readDeclaredField(
                                primaryLoadManager, "antiAffinityGroupPolicyFilter", true);
        var context = ((ExtensibleLoadManagerImpl) primaryLoadManager).getContext();
        var brokers = brokerRegistry
                .getAvailableBrokerLookupDataAsync().get(5, TimeUnit.SECONDS);
        ServiceUnitId namespaceBundle = mock(ServiceUnitId.class);
        doReturn(namespace + "/" + bundle).when(namespaceBundle).toString();

        var expected = new HashMap<>(brokers);
        var actual = antiAffinityGroupPolicyFilter.filterAsync(
                brokers, namespaceBundle, context).get();
        assertEquals(actual, expected);

        doReturn(antiAffinityEnabledNameSpace + "/" + bundle).when(namespaceBundle).toString();
        var serviceUnitStateChannel = (ServiceUnitStateChannel)
                FieldUtils.readDeclaredField(
                        primaryLoadManager, "serviceUnitStateChannel", true);
        var srcBroker = serviceUnitStateChannel.getOwnerAsync(namespaceBundle.toString())
                .get(5, TimeUnit.SECONDS).get();
        expected.remove(srcBroker);
        actual = antiAffinityGroupPolicyFilter.filterAsync(
                brokers, namespaceBundle, context).get();
        assertEquals(actual, expected);
    }
}
