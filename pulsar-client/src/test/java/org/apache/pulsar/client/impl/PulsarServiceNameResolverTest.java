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
    public void testResolveUrlBeforeUpdateServiceUrl() throws Exception {
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
