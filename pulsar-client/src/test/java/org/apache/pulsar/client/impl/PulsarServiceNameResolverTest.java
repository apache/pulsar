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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.common.net.ServiceURI;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link PulsarServiceNameResolver}.
 */
public class PulsarServiceNameResolverTest {

    private PulsarServiceNameResolver resolver;

    @BeforeMethod
    public void setup() {
        this.resolver = new PulsarServiceNameResolver();
        assertNull(resolver.getServiceUrl());
        assertNull(resolver.getServiceUri());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testResolveBeforeUpdateServiceUrl() {
        resolver.resolveHost();
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testResolveUrlBeforeUpdateServiceUrl() {
        resolver.resolveHostUri();
    }

    @Test
    public void testUpdateInvalidServiceUrl() {
        String serviceUrl = "pulsar:///";
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
}
