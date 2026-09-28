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
package org.apache.pulsar.broker.namespace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.coordination.impl.CoordinationServiceImpl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceServiceSplitFailureTest {

    @DataProvider(name = "splitFailureTiming")
    public Object[][] splitFailureTiming() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "splitFailureTiming")
    public void testSplitFailsWhenBundleCacheLoadFails(boolean alreadyFailed) throws Exception {
        @Cleanup
        PulsarTestContext context = PulsarTestContext.builderForNonStartableContext().spyByDefault().build();
        PulsarService pulsar = context.getPulsarService();
        @Cleanup
        CoordinationServiceImpl coordinationService = new CoordinationServiceImpl(context.getLocalMetadataStore());
        @Cleanup("shutdown")
        OrderedExecutor executor = OrderedExecutor.newBuilder().numThreads(1).name("split-failure-test").build();
        doReturn(coordinationService).when(pulsar).getCoordinationService();
        doReturn(executor).when(pulsar).getOrderedExecutor();
        assertThat(ExtensibleLoadManagerImpl.isLoadManagerExtensionEnabled(pulsar)).isFalse();

        @Cleanup
        NamespaceService namespaceService = new NamespaceService(pulsar);
        NamespaceName namespace = NamespaceName.get("prop/ns-split-cache-failure");
        NamespaceBundleFactory factory = namespaceService.getNamespaceBundleFactory();
        NamespaceBundle bundle = factory.getBundle(namespace.toString(), "0x00000000_0xffffffff");
        PulsarResources pulsarResources = pulsar.getPulsarResources();
        LocalPoliciesResources resources = spy(pulsarResources.getLocalPolicies());
        CompletableFuture<Optional<CacheGetResult<LocalPolicies>>> policiesFuture = new CompletableFuture<>();
        MetadataStoreException failure = new MetadataStoreException("Failed to read local policies for split");
        if (alreadyFailed) {
            policiesFuture.completeExceptionally(failure);
        }
        doReturn(policiesFuture).when(resources).getLocalPoliciesWithVersion(namespace);
        doReturn(resources).when(pulsarResources).getLocalPolicies();

        // The cache is empty: splitBundles must load policies through the real bundle factory.
        CompletableFuture<Void> result = namespaceService.splitAndOwnBundle(bundle, false,
                NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO, null);
        if (!alreadyFailed) {
            assertThat(result).isNotDone();
            policiesFuture.completeExceptionally(failure);
        }
        assertThatThrownBy(() -> result.get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(ServiceUnitNotReadyException.class)
                .hasMessageContaining(failure.getMessage());
        assertThat(result).isCompletedExceptionally();
    }
}
