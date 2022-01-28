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
package org.apache.pulsar.common.net;

import static org.testng.Assert.*;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import java.net.URI;
import org.testng.annotations.Test;

/**
 * Unit test {@link ServiceURI}.
 */
public class ServiceURITest {

    private static void assertServiceUri(
        String serviceUri,
        String expectedServiceName,
        String[] expectedServiceInfo,
        String expectedServiceUser,
        String[] expectedServiceHosts,
        String expectedServicePath) {

        ServiceURI serviceURI = ServiceURI.create(serviceUri);

        assertEquals(expectedServiceName, serviceURI.getServiceName());
        assertArrayEquals(expectedServiceInfo, serviceURI.getServiceInfos());
        assertEquals(expectedServiceUser, serviceURI.getServiceUser());
        assertArrayEquals(expectedServiceHosts, serviceURI.getServiceHosts());
        assertEquals(expectedServicePath, serviceURI.getServicePath());
    }

    @Test
    public void testInvalidServiceUris() {
        String[] uris = new String[] {
            "://localhost:6650",                // missing scheme
            "pulsar:///",                       // missing authority
            "pulsar://localhost:6650:6651/",    // invalid hostname pair
            "pulsar://localhost:xyz/",          // invalid port
            "pulsar://localhost:-6650/",        // negative port
            "pulsar://fec0:0:0:ffff::1:6650",   // missing brackets
        };

        for (String uri : uris) {
            testInvalidServiceUri(uri);
        }
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullServiceUriString() {
        ServiceURI.create((String) null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullServiceUriInstance() {
        ServiceURI.create((URI) null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEmptyServiceUriString() {
        ServiceURI.create("");
    }

    private void testInvalidServiceUri(String serviceUri) {
        try {
            ServiceURI.create(serviceUri);
            fail("Should fail to parse service uri : " + serviceUri);
        } catch (IllegalArgumentException iae) {
            // expected
        }
    }

    @Test
    public void testMissingServiceName() {
        String serviceUri = "//localhost:6650/path/to/namespace";
        assertServiceUri(
            serviceUri,
            null, new String[0], null, new String[] { "localhost:6650" }, "/path/to/namespace");
    }

    @Test
    public void testEmptyPath() {
        String serviceUri = "pulsar://localhost:6650";
        assertServiceUri(
            serviceUri,
            "pulsar", new String[0], null, new String[] { "localhost:6650" }, "");
    }

    @Test
    public void testRootPath() {
        String serviceUri = "pulsar://localhost:6650/";
        assertServiceUri(
            serviceUri,
            "pulsar", new String[0], null, new String[] { "localhost:6650" }, "/");
    }

    @Test
    public void testUserInfo() {
        String serviceUri = "pulsar://pulsaruser@localhost:6650/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            "pulsaruser",
            new String[] { "localhost:6650" },
            "/path/to/namespace");
    }

    @Test
    public void testIpv6Uri() {
        String serviceUri = "pulsar://pulsaruser@[fec0:0:0:ffff::1]:6650/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            "pulsaruser",
            new String[] { "[fec0:0:0:ffff::1]:6650" },
            "/path/to/namespace");
    }

    @Test
    public void testIpv6UriWithoutPulsarPort() {
        String serviceUri = "pulsar://[fec0:0:0:ffff::1]/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            null,
            new String[] { "[fec0:0:0:ffff::1]:6650" },
            "/path/to/namespace");
    }

    @Test
    public void testMultiIpv6Uri() {
        String serviceUri = "pulsar://pulsaruser@[fec0:0:0:ffff::1]:6650,[fec0:0:0:ffff::2]:6650;[fec0:0:0:ffff::3]:6650/path/to/namespace";
        assertServiceUri(
                serviceUri,
                "pulsar",
                new String[0],
                "pulsaruser",
                new String[] { "[fec0:0:0:ffff::1]:6650", "[fec0:0:0:ffff::2]:6650", "[fec0:0:0:ffff::3]:6650" },
                "/path/to/namespace");
    }

    @Test
    public void testMultiIpv6UriWithoutPulsarPort() {
        String serviceUri = "pulsar://pulsaruser@[fec0:0:0:ffff::1],[fec0:0:0:ffff::2];[fec0:0:0:ffff::3]/path/to/namespace";
        assertServiceUri(
                serviceUri,
                "pulsar",
                new String[0],
                "pulsaruser",
                new String[] { "[fec0:0:0:ffff::1]:6650", "[fec0:0:0:ffff::2]:6650", "[fec0:0:0:ffff::3]:6650" },
                "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsSemiColon() {
        String serviceUri = "pulsar://host1:6650;host2:6650;host3:6650/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            null,
            new String[] { "host1:6650", "host2:6650", "host3:6650" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsComma() {
        String serviceUri = "pulsar://host1:6650,host2:6650,host3:6650/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            null,
            new String[] { "host1:6650", "host2:6650", "host3:6650" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsWithoutPulsarPorts() {
        String serviceUri = "pulsar://host1,host2,host3/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            null,
            new String[] { "host1:6650", "host2:6650", "host3:6650" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsWithoutPulsarTlsPorts() {
        String serviceUri = "pulsar+ssl://host1,host2,host3/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[] { "ssl" },
            null,
            new String[] { "host1:6651", "host2:6651", "host3:6651" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsWithoutHttpPorts() {
        String serviceUri = "http://host1,host2,host3/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "http",
            new String[0],
            null,
            new String[] { "host1:80", "host2:80", "host3:80" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsWithoutHttpsPorts() {
        String serviceUri = "https://host1,host2,host3/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "https",
            new String[0],
            null,
            new String[] { "host1:443", "host2:443", "host3:443" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsMixedPorts() {
        String serviceUri = "pulsar://host1:6640,host2:6650,host3:6660/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            null,
            new String[] { "host1:6640", "host2:6650", "host3:6660" },
            "/path/to/namespace");
    }

    @Test
    public void testMultipleHostsMixed() {
        String serviceUri = "pulsar://host1:6640,host2,host3:6660/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            null,
            new String[] { "host1:6640", "host2:6650", "host3:6660" },
            "/path/to/namespace");
    }

    @Test
    public void testUserInfoWithMultipleHosts() {
        String serviceUri = "pulsar://pulsaruser@host1:6650;host2:6650;host3:6650/path/to/namespace";
        assertServiceUri(
            serviceUri,
            "pulsar",
            new String[0],
            "pulsaruser",
            new String[] { "host1:6650", "host2:6650", "host3:6650" },
            "/path/to/namespace");
    }

    @Test
    public void testSelectOneSingleHost() {
        String serviceUri = "https://host1:6650/path/to/namespace";
        assertEquals(ServiceURI.create(serviceUri).selectOne(),
                     serviceUri);
    }

    @Test
    public void testSelectOneMultipleHosts() {
        String serviceUri = "https://host1:6650;host2/";
        for (int i = 0; i < 10; i++) {
            String selected = ServiceURI.create(serviceUri).selectOne();
            boolean option1 = selected.equals("https://host1:6650/");
            boolean option2 = selected.equals("https://host2:443/");
            assertTrue(option1 || option2);
        }
    }

    @Test
    public void testSelectOneAllBellsAndWhistles() {
        String serviceUri = "https+blah://user1@host1:6650;host2;host3:4032/path/to/namespace";
        for (int i = 0; i < 10; i++) {
            String selected = ServiceURI.create(serviceUri).selectOne();
            boolean option1 = selected.equals("https+blah://user1@host1:6650/path/to/namespace");
            boolean option2 = selected.equals("https+blah://user1@host2:443/path/to/namespace");
            boolean option3 = selected.equals("https+blah://user1@host3:4032/path/to/namespace");
            assertTrue(option1 || option2 || option3);
        }
    }

    @Test
    public void testKubeProxyURI() {
        String serviceUri = "http://localhost:57777/api/v1/namespaces/blah-blah/services/pulsar:8080/proxy";
        assertEquals(ServiceURI.create(serviceUri).selectOne(),
                     serviceUri);
    }
}
