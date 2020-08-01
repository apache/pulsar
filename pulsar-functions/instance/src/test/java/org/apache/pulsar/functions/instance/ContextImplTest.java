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
package org.apache.pulsar.functions.instance;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.prometheus.client.CollectorRegistry;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.secretsprovider.EnvironmentBasedSecretsProvider;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link ContextImpl}.
 */
public class ContextImplTest {

    private InstanceConfig config;
    private Logger logger;
    private PulsarClientImpl client;
    private ContextImpl context;
    private Producer producer = mock(Producer.class);

    @BeforeMethod
    public void setup() {
        config = new InstanceConfig();
        FunctionDetails functionDetails = FunctionDetails.newBuilder()
            .setUserConfig("")
            .build();
        config.setFunctionDetails(functionDetails);
        logger = mock(Logger.class);
        client = mock(PulsarClientImpl.class);
        when(client.newProducer()).thenReturn(new ProducerBuilderImpl(client, Schema.BYTES));
        when(client.createProducerAsync(any(ProducerConfigurationData.class), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(producer));
        when(client.getSchema(anyString())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(producer.sendAsync(anyString())).thenReturn(CompletableFuture.completedFuture(null));

        TypedMessageBuilder messageBuilder = spy(new TypedMessageBuilderImpl(mock(ProducerBase.class), Schema.STRING));
        doReturn(new CompletableFuture<>()).when(messageBuilder).sendAsync();
        when(producer.newMessage()).thenReturn(messageBuilder);
        context = new ContextImpl(
            config,
            logger,
            client,
            new EnvironmentBasedSecretsProvider(), new CollectorRegistry(), new String[0],
                FunctionDetails.ComponentType.FUNCTION, null, null);
        context.setCurrentMessageContext((Record<String>) () -> null);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testIncrCounterStateDisabled() {
        context.incrCounter("test-key", 10);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testGetCounterStateDisabled() {
        context.getCounter("test-key");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testPutStateStateDisabled() {
        context.putState("test-key", ByteBuffer.wrap("test-value".getBytes(UTF_8)));
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDeleteStateStateDisabled() {
        context.deleteState("test-key");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testGetStateStateDisabled() {
        context.getState("test-key");
    }

    @Test
    public void testIncrCounterStateEnabled() throws Exception {
        context.stateContext = mock(StateContextImpl.class);
        context.incrCounterAsync("test-key", 10L);
        verify(context.stateContext, times(1)).incrCounter(eq("test-key"), eq(10L));
    }

    @Test
    public void testGetCounterStateEnabled() throws Exception {
        context.stateContext = mock(StateContextImpl.class);
        context.getCounterAsync("test-key");
        verify(context.stateContext, times(1)).getCounter(eq("test-key"));
    }

    @Test
    public void testPutStateStateEnabled() throws Exception {
        context.stateContext = mock(StateContextImpl.class);
        ByteBuffer buffer = ByteBuffer.wrap("test-value".getBytes(UTF_8));
        context.putStateAsync("test-key", buffer);
        verify(context.stateContext, times(1)).put(eq("test-key"), same(buffer));
    }

    @Test
    public void testDeleteStateStateEnabled() throws Exception {
        context.stateContext = mock(StateContextImpl.class);
        ByteBuffer buffer = ByteBuffer.wrap("test-value".getBytes(UTF_8));
        context.deleteStateAsync("test-key");
        verify(context.stateContext, times(1)).delete(eq("test-key"));
    }

    @Test
    public void testGetStateStateEnabled() throws Exception {
        context.stateContext = mock(StateContextImpl.class);
        context.getStateAsync("test-key");
        verify(context.stateContext, times(1)).get(eq("test-key"));
    }

    @Test
    public void testPublishUsingDefaultSchema() throws Exception {
        context.newOutputMessage("sometopic", null).value("Somevalue").sendAsync();
    }
 }
