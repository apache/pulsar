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
package org.apache.pulsar.compaction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.channel.EventLoopGroup;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.storage.BookKeeperClientContext;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;
import picocli.CommandLine.Option;

/**
 * CompactorTool Tests.
 */
public class CompactorToolTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testBookKeeperClientContextUsesNamespaceAffinity() throws Exception {
        ServiceConfiguration brokerConfig = new ServiceConfiguration();
        BookKeeperClientFactory bkClientFactory = mock(BookKeeperClientFactory.class);
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        BookKeeper bookKeeper = mock(BookKeeper.class);
        TopicName topicName = TopicName.get("persistent://tenant/namespace/topic");
        BookieAffinityGroupData affinityGroup = BookieAffinityGroupData.builder()
                .bookkeeperAffinityGroupPrimary("primary")
                .bookkeeperAffinityGroupSecondary("secondary")
                .build();
        Optional<LocalPolicies> localPolicies = Optional.of(new LocalPolicies(null, affinityGroup, null));

        when(bkClientFactory.create(same(brokerConfig), same(store), same(eventLoopGroup), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(bookKeeper));

        BookKeeperClientContext context = CompactorTool.createBookKeeperClientContext(
                brokerConfig, bkClientFactory, store, eventLoopGroup, topicName, localPolicies).get();

        ArgumentCaptor<Optional<Class<? extends EnsemblePlacementPolicy>>> policyClass =
                ArgumentCaptor.forClass(Optional.class);
        ArgumentCaptor<Map<String, Object>> policyProperties = ArgumentCaptor.forClass(Map.class);
        verify(bkClientFactory).create(same(brokerConfig), same(store), same(eventLoopGroup),
                policyClass.capture(), policyProperties.capture());
        assertEquals(policyClass.getValue(), Optional.of(IsolatedBookieEnsemblePlacementPolicy.class));
        assertEquals(policyProperties.getValue().get(
                IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS), "primary");
        assertEquals(policyProperties.getValue().get(
                IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS), "secondary");
        assertSame(context.getBookKeeper(), bookKeeper);

        byte[] encodedPolicy = context.withPlacementMetadata(Collections.emptyMap())
                .get(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG);
        EnsemblePlacementPolicyConfig decodedPolicy = EnsemblePlacementPolicyConfig.decode(encodedPolicy);
        assertEquals(decodedPolicy.getPolicyClass(), IsolatedBookieEnsemblePlacementPolicy.class);
        assertEquals(decodedPolicy.getProperties(), policyProperties.getValue());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testBookKeeperClientContextPreservesDefaultPlacementWithoutAffinity() throws Exception {
        ServiceConfiguration brokerConfig = new ServiceConfiguration();
        BookKeeperClientFactory bkClientFactory = mock(BookKeeperClientFactory.class);
        MetadataStoreExtended store = mock(MetadataStoreExtended.class);
        EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
        BookKeeper bookKeeper = mock(BookKeeper.class);
        when(bkClientFactory.create(same(brokerConfig), same(store), same(eventLoopGroup), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(bookKeeper));

        BookKeeperClientContext context = CompactorTool.createBookKeeperClientContext(
                brokerConfig, bkClientFactory, store, eventLoopGroup,
                TopicName.get("persistent://tenant/namespace/topic"), Optional.empty()).get();

        ArgumentCaptor<Optional<Class<? extends EnsemblePlacementPolicy>>> policyClass =
                ArgumentCaptor.forClass(Optional.class);
        ArgumentCaptor<Map<String, Object>> policyProperties = ArgumentCaptor.forClass(Map.class);
        verify(bkClientFactory).create(same(brokerConfig), same(store), same(eventLoopGroup),
                policyClass.capture(), policyProperties.capture());
        assertEquals(policyClass.getValue(), Optional.empty());
        assertNull(policyProperties.getValue());
        assertSame(context.getBookKeeper(), bookKeeper);
        assertFalse(context.withPlacementMetadata(Collections.emptyMap())
                .containsKey(EnsemblePlacementPolicyConfig.ENSEMBLE_PLACEMENT_POLICY_CONFIG));
    }

    /**
     * Test broker-tool generate docs.
     *
     * @throws Exception
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testGenerateDocs() throws Exception {
        PrintStream oldStream = System.out;
        try {
            ByteArrayOutputStream baoStream = new ByteArrayOutputStream();
            System.setOut(new PrintStream(baoStream));

            Class argumentsClass = Class.forName("org.apache.pulsar.compaction.CompactorTool$Arguments");

            Constructor constructor = argumentsClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            Object obj = constructor.newInstance();

            CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
            cmd.addCommand("compact-topic", obj);
            cmd.run(null);

            String message = baoStream.toString();

            Field[] fields = argumentsClass.getDeclaredFields();
            for (Field field : fields) {
                boolean fieldHasAnno = field.isAnnotationPresent(Option.class);
                if (fieldHasAnno) {
                    Option fieldAnno = field.getAnnotation(Option.class);
                    String[] names = fieldAnno.names();
                    String nameStr = Arrays.asList(names).toString();
                    nameStr = nameStr.substring(1, nameStr.length() - 1);
                    assertTrue(message.indexOf(nameStr) > 0);
                }
            }
        } finally {
            System.setOut(oldStream);
        }
    }

    @Test
    public void testUseTlsUrlWithPEM() throws PulsarClientException {
        ServiceConfiguration serviceConfiguration = spy(ServiceConfiguration.class);
        serviceConfiguration.setBrokerServicePortTls(Optional.of(6651));
        serviceConfiguration.setBrokerClientTlsEnabled(true);
        serviceConfiguration.setProperties(new Properties());

        @Cleanup
        PulsarClient ignored = CompactorTool.createClient(serviceConfiguration);

        verify(serviceConfiguration, times(1)).isBrokerClientTlsEnabled();
        verify(serviceConfiguration, times(1)).isTlsAllowInsecureConnection();
        verify(serviceConfiguration, times(1)).getBrokerClientKeyFilePath();
        verify(serviceConfiguration, times(1)).getBrokerClientTrustCertsFilePath();
        verify(serviceConfiguration, times(1)).getBrokerClientCertificateFilePath();
        serviceConfiguration.setBrokerClientTlsTrustStorePassword(MockedPulsarServiceBaseTest.BROKER_KEYSTORE_PW);
    }

    @Test
    public void testUseTlsUrlWithKeystore() throws PulsarClientException {
        ServiceConfiguration serviceConfiguration = spy(ServiceConfiguration.class);
        serviceConfiguration.setBrokerServicePortTls(Optional.of(6651));
        serviceConfiguration.setBrokerClientTlsEnabled(true);
        serviceConfiguration.setBrokerClientTlsEnabledWithKeyStore(true);
        serviceConfiguration.setBrokerClientTlsTrustStore(MockedPulsarServiceBaseTest.BROKER_KEYSTORE_FILE_PATH);

        serviceConfiguration.setProperties(new Properties());

        @Cleanup
        PulsarClient ignored = CompactorTool.createClient(serviceConfiguration);

        verify(serviceConfiguration, times(1)).isBrokerClientTlsEnabled();
        verify(serviceConfiguration, times(1)).isBrokerClientTlsEnabledWithKeyStore();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsKeyStore();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsKeyStorePassword();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsKeyStoreType();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsTrustStore();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsTrustStorePassword();
        verify(serviceConfiguration, times(1)).getBrokerClientTlsTrustStoreType();
    }
}
