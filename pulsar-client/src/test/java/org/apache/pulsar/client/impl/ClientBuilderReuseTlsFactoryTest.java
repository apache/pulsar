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
import javax.net.ssl.SSLContext;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.Test;

/**
 * PIP-478: each {@link PulsarClient} owns the TLS factory it composes.
 *
 * <p>A client stores the factory it composes during construction onto its {@code ClientConfigurationData}
 * (see {@code PulsarClientImpl.setupClientTlsFactory}) and closes that factory on shutdown. While
 * {@code ClientBuilderImpl.build()} handed the builder's own configuration object to the client, that made
 * the factory shared state on an object the caller still held: a second {@code build()} adopted and
 * re-initialized the first client's live factory, and closing either client then closed TLS for the other
 * — permanently, while the surviving client still reported itself open. Building more than one client from
 * one builder is ordinary usage, so {@code build()} now hands over a copy of the configuration.
 */
public class ClientBuilderReuseTlsFactoryTest {

    private static ClientBuilder tlsBuilder() {
        return PulsarClient.builder()
                .serviceUrl("pulsar+ssl://localhost:6651")
                .allowTlsInsecureConnection(true);
    }

    @Test
    public void aBuilderIsStillUsableAfterItsClientIsClosed() throws Exception {
        ClientBuilder builder = tlsBuilder();

        PulsarClient first = builder.build();
        first.close();

        assertThatCode(() -> builder.build().close())
                .as("a second client from the same builder must compose a fresh TLS factory")
                .doesNotThrowAnyException();
    }

    /** The builder's own configuration must never be mutated with a factory the client composed. */
    @Test
    public void buildingDoesNotStoreTheComposedFactoryOnTheBuilder() throws Exception {
        ClientBuilderImpl builder = (ClientBuilderImpl) tlsBuilder();

        PulsarClient client = builder.build();
        try {
            assertThat(builder.getClientConfigurationData().getTlsFactory())
                    .as("the composed factory belongs to the client, not to the caller's configuration")
                    .isNull();
            assertThat(((PulsarClientImpl) client).getConfiguration().getTlsFactory())
                    .as("the client does hold one")
                    .isNotNull();
        } finally {
            client.close();
        }
    }

    /**
     * Two clients from one builder must not share a factory — otherwise closing the first one closes TLS
     * for the second, which keeps reporting itself open while every connect, reconnect and HTTPS lookup
     * fails with "FileBasedTlsFactory is closed".
     */
    @Test
    public void twoClientsFromOneBuilderDoNotShareATlsFactory() throws Exception {
        ClientBuilder builder = tlsBuilder();

        PulsarClient first = builder.build();
        PulsarClient second = builder.build();
        try {
            PulsarTlsFactory firstFactory = ((PulsarClientImpl) first).getConfiguration().getTlsFactory();
            PulsarTlsFactory secondFactory = ((PulsarClientImpl) second).getConfiguration().getTlsFactory();
            assertThat(firstFactory).isNotNull();
            assertThat(secondFactory).as("each client composes its own").isNotSameAs(firstFactory);

            first.close();

            assertThat(secondFactory.createInstance(TlsPurpose.CLIENT_DEFAULT, SSLContext.class).get())
                    .as("closing the first client must leave the second client's TLS working")
                    .isPresent();
        } finally {
            second.close();
        }
    }
}
