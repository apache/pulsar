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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.pulsar.client.api.PulsarClientException.InvalidServiceURL;
import org.apache.pulsar.common.net.ServiceURI;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link PulsarServiceNameResolver}.
 */
public class PulsarServiceNameResolverTest {

    private PulsarServiceNameResolver resolver;
    private ServerSocket serverSocket;
    private boolean closed = false;
    @BeforeMethod
    public void setup() {
        this.resolver = new PulsarServiceNameResolver();
        assertNull(resolver.getServiceUrl());
        assertNull(resolver.getServiceUri());
    }

    @BeforeClass
    public void init() throws IOException {
        InetAddress inetAddress = InetAddress.getByName("0.0.0.0");
        serverSocket = new ServerSocket(6666, 10, inetAddress);
        new Thread(() -> {
            while (!closed) {
                try {
                    serverSocket.accept();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
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

    @Test
    public void testServiceUrlHealthCheck() throws Exception {
        String serviceUrl = "pulsar+ssl://host1:6651,host2:6651,127.0.0.1:6666";
        resolver.updateServiceUrl(serviceUrl);
        assertEquals(serviceUrl, resolver.getServiceUrl());
        assertEquals(ServiceURI.create(serviceUrl), resolver.getServiceUri());

        Set<InetSocketAddress> expectedAddresses = new HashSet<>();
        Set<URI> expectedHostUrls = new HashSet<>();
        expectedAddresses.add(InetSocketAddress.createUnresolved("host1", 6651));
        expectedAddresses.add(InetSocketAddress.createUnresolved("host2", 6651));
        expectedAddresses.add(InetSocketAddress.createUnresolved("127.0.0.1", 6666));
        expectedHostUrls.add(URI.create("pulsar+ssl://host1:6651"));
        expectedHostUrls.add(URI.create("pulsar+ssl://host2:6651"));
        expectedHostUrls.add(URI.create("pulsar+ssl://127.0.0.1:6666"));

        for (int i = 0; i < 10; i++) {
            assertTrue(expectedAddresses.contains(resolver.resolveHost()));
            assertTrue(expectedHostUrls.contains(resolver.resolveHostUri()));
        }
        // wait for health check to complete
        Uninterruptibles.sleepUninterruptibly(30, java.util.concurrent.TimeUnit.SECONDS);
        // check if the unhealthy address is removed
        Set<InetSocketAddress> expectedHealthyAddresses = new HashSet<>();
        Set<URI> expectedHealthyHostUrls = new HashSet<>();
        expectedHealthyAddresses.add(InetSocketAddress.createUnresolved("127.0.0.1", 6666));
        expectedHealthyHostUrls.add(URI.create("pulsar+ssl://127.0.0.1:6666"));

        for (int i = 0; i < 10; i++) {
            assertTrue(expectedHealthyAddresses.contains(resolver.resolveHost()));
            assertTrue(expectedHealthyHostUrls.contains(resolver.resolveHostUri()));
        }
    }

    @AfterClass
    public void close() throws IOException {
        resolver.close();
        serverSocket.close();
        closed = true;
    }
}
