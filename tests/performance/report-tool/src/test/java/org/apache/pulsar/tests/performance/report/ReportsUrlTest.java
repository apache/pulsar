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
package org.apache.pulsar.tests.performance.report;

import static org.assertj.core.api.Assertions.assertThat;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.List;
import org.testng.annotations.Test;

public class ReportsUrlTest {
    private static final Path ROOT = Path.of("/home/user/pulsar/build/performance");

    @Test
    public void usesTheConfiguredBaseUrlWithATrailingSlash() {
        assertThat(ReportsUrl.baseUrl("http://192.168.1.123:8000", "0.0.0.0", 8000))
                .isEqualTo("http://192.168.1.123:8000/");
        assertThat(ReportsUrl.baseUrl("https://reports.example.com/perf/", null, 8000))
                .isEqualTo("https://reports.example.com/perf/");
    }

    @Test
    public void usesTheBindAddressAndPortWithoutABaseUrl() {
        assertThat(ReportsUrl.baseUrl(null, null, 8000)).isEqualTo("http://127.0.0.1:8000/");
        assertThat(ReportsUrl.baseUrl(" ", "10.1.2.3", 9000)).isEqualTo("http://10.1.2.3:9000/");
        assertThat(ReportsUrl.baseUrl(null, "fd00::1", 8000)).isEqualTo("http://[fd00::1]:8000/");
    }

    @Test
    public void choosesTheFirstNetworkInterfacesIpv4AddressForTheWildcardAddress() throws Exception {
        List<ReportsUrl.Interface> interfaces = List.of(
                new ReportsUrl.Interface(13, true, false, List.of(InetAddress.getByName("172.17.0.1"))),
                new ReportsUrl.Interface(1, true, true, List.of(InetAddress.getByName("127.0.0.1"))),
                new ReportsUrl.Interface(3, false, false, List.of(InetAddress.getByName("10.9.9.9"))),
                new ReportsUrl.Interface(2, true, false, List.of(InetAddress.getByName("fe80::1"),
                        InetAddress.getByName("169.254.1.1"), InetAddress.getByName("192.168.1.123"))));

        // The interface with the lowest index that is up, and its address that isn't IPv6 or link-local
        assertThat(ReportsUrl.firstIpv4Address(interfaces)).hasValue("192.168.1.123");
        assertThat(ReportsUrl.firstIpv4Address(List.of())).isEmpty();
        assertThat(ReportsUrl.isWildcard("0.0.0.0")).isTrue();
        assertThat(ReportsUrl.isWildcard("127.0.0.1")).isFalse();
    }

    @Test
    public void appendsAReportsPathInTheRootWithoutItsIndexPage() {
        String baseUrl = "http://192.168.1.123:8000/";

        assertThat(ReportsUrl.url(baseUrl, ROOT,
                ROOT.resolve("2026-09-27/master/iot-telemetry/09-27-12-00-00/index.html")))
                .hasValue("http://192.168.1.123:8000/2026-09-27/master/iot-telemetry/09-27-12-00-00/");
        assertThat(ReportsUrl.url(baseUrl, ROOT, ROOT.resolve("2026-09-27/master/run/off-cpu/digest.html")))
                .hasValue("http://192.168.1.123:8000/2026-09-27/master/run/off-cpu/digest.html");
        assertThat(ReportsUrl.url(baseUrl, ROOT, ROOT.resolve("2026-09-27/my experiment/index.html")))
                .hasValue("http://192.168.1.123:8000/2026-09-27/my%20experiment/");
        // A run written with --output outside the root has no URL on the server
        assertThat(ReportsUrl.url(baseUrl, ROOT, Path.of("/tmp/run/index.html"))).isEmpty();
    }
}
