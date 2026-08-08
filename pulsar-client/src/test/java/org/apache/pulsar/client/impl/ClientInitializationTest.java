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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationHttpClientFactory;
import org.apache.pulsar.client.api.AuthenticationInitContext;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testng.annotations.Test;

public class ClientInitializationTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testInitializeAuthWithTls() throws PulsarClientException {
        Authentication auth = mock(Authentication.class);

        @Cleanup
        PulsarClient pulsarClient =
                PulsarClient.builder()
                .serviceUrl("pulsar+ssl://my-host:6650")
                .authentication(auth)
                .build();

        verify(auth).start(any(AuthenticationInitContext.class));
        verify(auth, times(0)).getAuthData();
    }

    @Test
    public void testInitializeAuthWithContextContainingHttpClientFactory() throws PulsarClientException {
        ContextCapturingAuthentication auth = new ContextCapturingAuthentication();

        @Cleanup
        PulsarClient pulsarClient =
                PulsarClient.builder()
                        .serviceUrl("pulsar://my-host:6650")
                        .authentication(auth)
                        .build();

        assertThat(auth.initContext).isNotNull();
        assertThat(auth.initContext.getService(AuthenticationHttpClientFactory.class)).isPresent();
        assertThat(auth.initContext.getServiceByName(AuthenticationHttpClientFactory.class, "default")).isPresent();
    }

    private static final class ContextCapturingAuthentication implements Authentication {
        private AuthenticationInitContext initContext;

        @Override
        public String getAuthMethodName() {
            return "context-capturing";
        }

        @Override
        public void configure(Map<String, String> authParams) {
            // No-op.
        }

        @Override
        public void start() throws PulsarClientException {
            // No-op.
        }

        @Override
        public void start(AuthenticationInitContext context) throws PulsarClientException {
            this.initContext = context;
        }

        @Override
        public void close() throws IOException {
            // No-op.
        }
    }
}
