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
package org.apache.pulsar.client.admin.internal;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.testng.Assert.assertEquals;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
import lombok.Cleanup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PulsarAdminGzipTest {
    WireMockServer server;

    @BeforeClass(alwaysRun = true)
    void beforeClass() throws IOException {
        server = new WireMockServer(WireMockConfiguration.wireMockConfig()
                .port(0));
        server.start();
    }

    @AfterClass(alwaysRun = true)
    void afterClass() {
        if (server != null) {
            server.stop();
        }
    }

    static byte[] gzipContent(String content) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try(GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
            gzipOutputStream.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return byteArrayOutputStream.toByteArray();
    }

    @AfterMethod
    void resetAllMocks() {
        server.resetAll();
    }

    @Test
    public void testGzipRequestedGzipResponse() throws Exception {
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .withHeader("Accept-Encoding", equalTo("gzip"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withHeader("Content-Encoding", "gzip")
                        .withBody(gzipContent("[\"gzip-test\", \"gzip-test2\"]"))));

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:" + server.port())
                .acceptGzipCompression(true)
                .build();

        assertEquals(admin.clusters().getClusters(), Arrays.asList("gzip-test", "gzip-test2"));
    }

    @Test
    public void testGzipRequestedNoGzipResponse() throws Exception {
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .withHeader("Accept-Encoding", equalTo("gzip"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test\", \"test2\"]")));

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:" + server.port())
                .acceptGzipCompression(true)
                .build();

        assertEquals(admin.clusters().getClusters(), Arrays.asList("test", "test2"));
    }

    @Test
    public void testNoGzipRequestedNoGzipResponse() throws Exception {
        server.stubFor(get(urlEqualTo("/admin/v2/clusters"))
                .withHeader("Accept-Encoding", absent())
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("[\"test\", \"test2\"]")));

        @Cleanup
        PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://localhost:" + server.port())
                .acceptGzipCompression(false)
                .build();

        assertEquals(admin.clusters().getClusters(), Arrays.asList("test", "test2"));
    }
}
