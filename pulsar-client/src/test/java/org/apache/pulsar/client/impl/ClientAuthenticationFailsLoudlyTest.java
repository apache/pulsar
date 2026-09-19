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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.auth.AuthenticationInitContext;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

/**
 * PIP-478: two failure modes that used to be loud must stay loud after the v5-native inversion.
 *
 * <p>Both were carried by {@code V5ToV4AuthenticationAdapter}, the v5&rarr;v4 wrapper the inversion deleted.
 * Neither is exotic: one is a plugin that cannot serve the transport it is configured for, the other is a
 * configuration crossing a process boundary. Left silent, the first fails every connection for the client's
 * lifetime with the reason buried in a connection error, and the second authenticates as nobody.
 */
public class ClientAuthenticationFailsLoudlyTest {

    /**
     * The binary transport requires {@code BinaryAuthDataProvider} (PIP-478 binary routing rule 1). A plugin
     * that cannot serve it is a build-time mistake and is reported as one, rather than as a connection
     * failure repeated forever.
     */
    @Test
    public void aPluginThatCannotServeTheBinaryTransportFailsTheBuild() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setV5Authentication(new HttpOnlyAuthentication());

        assertThatThrownBy(() -> new PulsarClientImpl(conf))
                .isInstanceOf(PulsarClientException.UnsupportedAuthenticationException.class)
                .hasMessageContaining("BinaryAuthDataProvider");
    }

    /**
     * A v5 plugin is not serializable and its slot is transient, so serializing a configuration carrying one
     * would drop it without a word — and the deserialized client would authenticate as nobody. Refuse
     * instead, pointing at the string form that does survive.
     */
    @Test
    public void serializingAConfigurationCarryingAV5PluginIsRefused() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setV5Authentication(new HttpOnlyAuthentication());

        assertThatThrownBy(() -> serialize(conf))
                .isInstanceOf(NotSerializableException.class)
                .hasMessageContaining("authPluginClassName");
    }

    /** The string form survives serialization, so a configuration using it is written normally. */
    @Test
    public void aConfigurationUsingTheStringFormStillSerializes() throws Exception {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar://localhost:6650");
        conf.setAuthPluginClassName("org.apache.pulsar.client.impl.auth.AuthenticationToken");
        conf.setAuthParams("token:the-jwt");
        conf.setV5Authentication(new HttpOnlyAuthentication());

        assertThat(serialize(conf)).isPositive();
    }

    private static int serialize(ClientConfigurationData conf) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(conf);
        }
        return bytes.size();
    }

    /** A plugin that serves no binary capability at all. */
    private static final class HttpOnlyAuthentication implements Authentication {

        @Override
        public CompletableFuture<Void> initializeAsync(AuthenticationInitContext ctx) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> Optional<T> capability(Class<T> kind) {
            return Optional.empty();
        }
    }
}
