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
package org.apache.pulsar.client.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.annotations.Test;

/**
 * PIP-478: a {@link ClientBuilder} stays reusable after the client it built is closed.
 *
 * <p>{@code ClientBuilderImpl.build()} hands the builder's own {@code ClientConfigurationData} to the client
 * without cloning it, so the TLS factory the client composes during construction is written into an object
 * the caller still holds. Closing the client closes that factory; if the slot is not also cleared, the next
 * {@code build()} from the same builder mistakes the dead factory for one adopted through the v5 builder,
 * re-initializes it and fails. Reusing a builder is ordinary v4 usage that worked before PIP-478.
 */
public class ClientBuilderReuseTlsFactoryTest {

    @Test
    public void aBuilderIsStillUsableAfterItsClientIsClosed() throws Exception {
        ClientBuilder builder = PulsarClient.builder().serviceUrl("pulsar+ssl://localhost:6651");

        PulsarClient first = builder.build();
        first.close();

        assertThatCode(() -> builder.build().close())
                .as("a second client from the same builder must compose a fresh TLS factory")
                .doesNotThrowAnyException();
    }

    /** The composed factory must not be left behind on the caller's configuration at all. */
    @Test
    public void closingTheClientClearsTheComposedFactoryFromTheConfiguration() throws Exception {
        ClientBuilderImpl builder = (ClientBuilderImpl) PulsarClient.builder()
                .serviceUrl("pulsar+ssl://localhost:6651");

        PulsarClient client = builder.build();
        assertThat(builder.getClientConfigurationData().getTlsFactory())
                .as("the client composes a factory and stores it on the shared configuration")
                .isNotNull();

        client.close();

        assertThat(builder.getClientConfigurationData().getTlsFactory())
                .as("and clears it again on close, so the builder is not left holding a closed factory")
                .isNull();
    }
}
