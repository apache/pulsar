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
package org.apache.pulsar.client.impl.auth.http;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Optional;
import org.apache.pulsar.client.api.AuthenticationInitContext;
import org.apache.pulsar.client.impl.auth.httpclient.AuthenticationHttpClientConfig;
import org.apache.pulsar.client.impl.auth.httpclient.AuthenticationHttpClientFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class AuthenticationHttpClientFactoryTest {

    @Test
    public void testCreateHttpClientWithDefaultConfig() throws IOException {
        AuthenticationHttpClientConfig config = AuthenticationHttpClientConfig.builder().build();
        AuthenticationHttpClientFactory factory = new AuthenticationHttpClientFactory(config, null);

        AsyncHttpClient httpClient = factory.createHttpClient();

        assertNotNull(httpClient);
        httpClient.close();
    }

    @Test
    public void testGetNameResolverWithContext() {
        AuthenticationHttpClientConfig config = AuthenticationHttpClientConfig.builder().build();
        AuthenticationInitContext context = Mockito.mock(AuthenticationInitContext.class);
        NameResolver<InetAddress> mockResolver = Mockito.mock(NameResolver.class);

        Mockito.when(context.getService(NameResolver.class))
                .thenReturn(Optional.of(mockResolver));

        AuthenticationHttpClientFactory factory = new AuthenticationHttpClientFactory(config, context);

        NameResolver<InetAddress> resolver = factory.getNameResolver();

        assertNotNull(resolver);
        assertEquals(resolver, mockResolver);
    }

    @Test
    public void testGetNameResolverWithoutContext() {
        AuthenticationHttpClientConfig config = AuthenticationHttpClientConfig.builder().build();
        AuthenticationHttpClientFactory factory = new AuthenticationHttpClientFactory(config, null);

        NameResolver<InetAddress> resolver = factory.getNameResolver();

        assertNull(resolver);
    }

    @Test
    public void testGetNameResolverWithContextButNoResolver() {
        AuthenticationHttpClientConfig config = AuthenticationHttpClientConfig.builder().build();
        AuthenticationInitContext context = Mockito.mock(AuthenticationInitContext.class);

        Mockito.when(context.getService(NameResolver.class))
                .thenReturn(Optional.empty());

        AuthenticationHttpClientFactory factory = new AuthenticationHttpClientFactory(config, context);

        NameResolver<InetAddress> resolver = factory.getNameResolver();

        assertNull(resolver);
    }

    @Test
    public void testHttpClientUsesSharedResources() throws Exception {
        AuthenticationHttpClientConfig config = AuthenticationHttpClientConfig.builder().build();
        AuthenticationInitContext context = Mockito.mock(AuthenticationInitContext.class);
        Timer mockTimer = Mockito.mock(Timer.class);

        Mockito.when(context.getService(Timer.class))
                .thenReturn(Optional.of(mockTimer));

        AuthenticationHttpClientFactory factory = new AuthenticationHttpClientFactory(config, context);

        AsyncHttpClient httpClient = factory.createHttpClient();
        assertNotNull(httpClient);
        assertEquals(httpClient.getConfig().getNettyTimer(), mockTimer);
        httpClient.close();
        mockTimer.stop();
    }

}
