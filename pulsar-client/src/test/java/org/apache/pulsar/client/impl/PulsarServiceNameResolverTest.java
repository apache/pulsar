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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.common.net.ServiceURI;
import org.testng.annotations.Test;

/**
 * Unit test {@link PulsarServiceNameResolver}.
 */
public class PulsarServiceNameResolverTest {

    public PulsarServiceNameResolver createResolver() {
        final PulsarServiceNameResolver resolver = new PulsarServiceNameResolver();
        assertNull(resolver.getServiceUrl());
        assertNull(resolver.getServiceUri());
        return resolver;
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testResolveBeforeUpdateServiceUrl() {
        createResolver().resolveHost();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testResolveUrlBeforeUpdateServiceUrl() {
        createResolver().resolveHostUri();
    }

    @Test
    public void testUpdateInvalidServiceUrl() {
        String serviceUrl = "pulsar:///";
        PulsarServiceNameResolver resolver = createResolver();
        try {
            resolver.updateServiceUrl(serviceUrl);
            fail("Should fail to update service url if service url is invalid");
        } catch (InvalidServiceURL isu) {
            // expected
        }
        assertNull(resolver.getServiceUrl());
        assertNull(resolver.getServiceUri());
    }

    @Test
    public void testSimpleHostUrl() throws Exception {
        String serviceUrl = "pulsar://host1:6650";
        PulsarServiceNameResolver resolver = createResolver();
        resolver.updateServiceUrl(serviceUrl);
        assertEquals(serviceUrl, resolver.getServiceUrl());
        assertEquals(ServiceURI.create(serviceUrl), resolver.getServiceUri());

        InetSocketAddress expectedAddress = InetSocketAddress.createUnresolved("host1", 6650);
        assertEquals(expectedAddress, resolver.resolveHost());
        assertEquals(URI.create(serviceUrl), resolver.resolveHostUri());

        String newServiceUrl = "pulsar://host2:6650";
        resolver.updateServiceUrl(newServiceUrl);
        assertEquals(newServiceUrl, resolver.getServiceUrl());
        assertEquals(ServiceURI.create(newServiceUrl), resolver.getServiceUri());

        InetSocketAddress newExpectedAddress = InetSocketAddress.createUnresolved("host2", 6650);
        assertEquals(newExpectedAddress, resolver.resolveHost());
        assertEquals(URI.create(newServiceUrl), resolver.resolveHostUri());
    }

    @Test
    public void testMultipleHostsUrl() throws Exception {
        String serviceUrl = "pulsar://host1:6650,host2:6650";
        PulsarServiceNameResolver resolver = createResolver();
        resolver.updateServiceUrl(serviceUrl);
        assertEquals(serviceUrl, resolver.getServiceUrl());
        assertEquals(ServiceURI.create(serviceUrl), resolver.getServiceUri());

        Set<InetSocketAddress> expectedAddresses = new HashSet<>();
        Set<URI> expectedHostUrls = new HashSet<>();
        expectedAddresses.add(InetSocketAddress.createUnresolved("host1", 6650));
        expectedAddresses.add(InetSocketAddress.createUnresolved("host2", 6650));
        expectedHostUrls.add(URI.create("pulsar://host1:6650"));
        expectedHostUrls.add(URI.create("pulsar://host2:6650"));

        for (int i = 0; i < 10; i++) {
            assertTrue(expectedAddresses.contains(resolver.resolveHost()));
            assertTrue(expectedHostUrls.contains(resolver.resolveHostUri()));
        }
    }

    @Test
    public void testMultipleHostsTlsUrl() throws Exception {
        String serviceUrl = "pulsar+ssl://host1:6651,host2:6651";
        PulsarServiceNameResolver resolver = createResolver();
        resolver.updateServiceUrl(serviceUrl);
        assertEquals(serviceUrl, resolver.getServiceUrl());
        assertEquals(ServiceURI.create(serviceUrl), resolver.getServiceUri());

        Set<InetSocketAddress> expectedAddresses = new HashSet<>();
        Set<URI> expectedHostUrls = new HashSet<>();
        expectedAddresses.add(InetSocketAddress.createUnresolved("host1", 6651));
        expectedAddresses.add(InetSocketAddress.createUnresolved("host2", 6651));
        expectedHostUrls.add(URI.create("pulsar+ssl://host1:6651"));
        expectedHostUrls.add(URI.create("pulsar+ssl://host2:6651"));

        for (int i = 0; i < 10; i++) {
            assertTrue(expectedAddresses.contains(resolver.resolveHost()));
            assertTrue(expectedHostUrls.contains(resolver.resolveHostUri()));
        }
    }

    @Test
    public void testMultipleHostsUrlUnreachable() throws Exception {
        String serviceUrl = "pulsar://host1:6650,host2:6650,host3:6650";

        PulsarServiceNameResolver resolver = new PulsarServiceNameResolver(
                address -> "host2".equals(address.getHostName())
                        || "host3".equals(address.getHostName())
                        || !address.isUnresolved());
        assertNull(resolver.getServiceUrl());
        assertNull(resolver.getServiceUri());

        resolver.updateServiceUrl(serviceUrl);
        assertEquals(serviceUrl, resolver.getServiceUrl());
        assertEquals(ServiceURI.create(serviceUrl), resolver.getServiceUri());

        List<InetSocketAddress> expectedAddresses = new ArrayList<>();
        Set<URI> expectedHostUrls = new HashSet<>();
        expectedAddresses.add(InetSocketAddress.createUnresolved("host2", 6650));
        expectedAddresses.add(InetSocketAddress.createUnresolved("host3", 6650));
        expectedHostUrls.add(URI.create("pulsar://host2:6650"));
        expectedHostUrls.add(URI.create("pulsar://host3:6650"));

        Set<InetSocketAddress> unexpectedAddresses = new HashSet<>();
        Set<URI> unexpectedHostUrls = new HashSet<>();
        unexpectedAddresses.add(InetSocketAddress.createUnresolved("host1", 6650));
        unexpectedHostUrls.add(URI.create("pulsar://host1:6650"));

        for (int i = 0; i < 10; i++) {
            assertThat(expectedAddresses).contains(resolver.resolveHost());
            assertThat(expectedHostUrls).contains(resolver.resolveHostUri());
            assertThat(unexpectedAddresses).doesNotContain(resolver.resolveHost());
            assertThat(unexpectedHostUrls).doesNotContain(resolver.resolveHostUri());
        }
    }
}
