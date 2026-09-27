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
package org.apache.pulsar.broker;

import static org.testng.Assert.assertEquals;
import java.net.URI;
import org.testng.annotations.Test;

public class ServiceConfigurationUtilsTest {

    @Test
    public void testBrokerUrlWithHostname() {
        assertEquals(ServiceConfigurationUtils.brokerUrl("broker-1.example.com", 6650),
                "pulsar://broker-1.example.com:6650");
    }

    @Test
    public void testBrokerUrlWithIpv4() {
        assertEquals(ServiceConfigurationUtils.brokerUrl("10.0.0.1", 6650), "pulsar://10.0.0.1:6650");
    }

    @Test
    public void testBrokerUrlWithIpv6Bare() {
        // A bare IPv6 literal must be wrapped in brackets so the resulting URL parses correctly.
        String url = ServiceConfigurationUtils.brokerUrl("::1", 6650);
        assertEquals(url, "pulsar://[::1]:6650");
        URI parsed = URI.create(url);
        assertEquals(parsed.getPort(), 6650);
    }

    @Test
    public void testBrokerUrlTlsWithIpv6Bare() {
        String url = ServiceConfigurationUtils.brokerUrlTls("fe80::1", 6651);
        assertEquals(url, "pulsar+ssl://[fe80::1]:6651");
        assertEquals(URI.create(url).getPort(), 6651);
    }

    @Test
    public void testWebServiceUrlWithIpv6Bare() {
        String url = ServiceConfigurationUtils.webServiceUrl("2001:db8::1", 8080);
        assertEquals(url, "http://[2001:db8::1]:8080");
        assertEquals(URI.create(url).getPort(), 8080);
    }

    @Test
    public void testWebServiceUrlTlsWithIpv6Bare() {
        String url = ServiceConfigurationUtils.webServiceUrlTls("2001:db8::1", 8443);
        assertEquals(url, "https://[2001:db8::1]:8443");
        assertEquals(URI.create(url).getPort(), 8443);
    }

    @Test
    public void testIpv6AlreadyBracketedIsNotDoubleWrapped() {
        // If the caller passed a pre-bracketed IPv6 literal, leave it as-is rather than producing
        // `pulsar://[[::1]]:6650`.
        String url = ServiceConfigurationUtils.brokerUrl("[::1]", 6650);
        assertEquals(url, "pulsar://[::1]:6650");
        assertEquals(URI.create(url).getPort(), 6650);
    }

    @Test
    public void testIpv4InUrlIsNotBracketed() {
        // IPv4 addresses must not be bracketed.
        String url = ServiceConfigurationUtils.webServiceUrl("0.0.0.0", 8080);
        assertEquals(url, "http://0.0.0.0:8080");
    }
}
