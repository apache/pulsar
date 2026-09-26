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
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReportsServerTest {
    private Path root;
    private HttpServer server;
    private HttpClient client;

    @BeforeMethod
    public void startServer() throws IOException {
        root = Files.createTempDirectory("reports-server-test");
        Path run = Files.createDirectories(root.resolve("2026-09-26/master/iot-telemetry-local/09-26-12-00-00"));
        Files.writeString(run.resolve("README.md"), "# Run report\n");
        Files.writeString(run.resolve("index.html"), "<h1>Run report</h1>");
        Files.writeString(run.resolve("resolved-config.yaml"), "cluster:\n  brokers: 1\n");
        Files.writeString(run.resolve("host-stats.csv"), "epochMillis,packageCelsius\n");
        Files.writeString(run.resolve("recording.jfr"), "not a recording");
        server = ReportsServer.start(root, new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
                OutputStream.nullOutputStream());
        client = HttpClient.newHttpClient();
    }

    @AfterMethod(alwaysRun = true)
    public void stopServer() throws IOException {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.stop(0);
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    @Test
    public void servesTheRunReportForTheRunDirectory() throws Exception {
        HttpResponse<String> response = get("/2026-09-26/master/iot-telemetry-local/09-26-12-00-00/");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).isEqualTo("<h1>Run report</h1>");
    }

    @Test
    public void servesTextFilesAsPlainText() throws Exception {
        for (String file : new String[] {"README.md", "resolved-config.yaml", "host-stats.csv"}) {
            HttpResponse<String> response = get("/2026-09-26/master/iot-telemetry-local/09-26-12-00-00/" + file);

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.headers().firstValue("Content-Type")).hasValue(ReportsServer.TEXT);
        }
    }

    @Test
    public void keepsTheContentTypeOfOtherFiles() throws Exception {
        HttpResponse<String> report = get("/2026-09-26/master/iot-telemetry-local/09-26-12-00-00/index.html");
        HttpResponse<String> recording = get("/2026-09-26/master/iot-telemetry-local/09-26-12-00-00/recording.jfr");

        assertThat(report.headers().firstValue("Content-Type")).hasValue("text/html");
        assertThat(recording.headers().firstValue("Content-Type")).hasValue("application/octet-stream");
    }

    @Test
    public void listsTheDirectories() throws Exception {
        HttpResponse<String> response = get("/2026-09-26/");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("master/");
    }

    @Test
    public void doesNotServeFilesOutsideTheRoot() throws Exception {
        HttpResponse<String> response = get("/../" + root.getFileName() + "/2026-09-26/");

        assertThat(response.statusCode()).isEqualTo(404);
    }

    @Test
    public void recognizesTheTextFileExtensions() {
        assertThat(ReportsServer.isTextFile("/run/producer/produce-latency.hgrm")).isTrue();
        assertThat(ReportsServer.isTextFile("/run/producer/container.log.txt")).isTrue();
        assertThat(ReportsServer.isTextFile("/run/RUN-REPORT.MD")).isTrue();
        assertThat(ReportsServer.isTextFile("/run/broker-profile/recording.jfr")).isFalse();
        assertThat(ReportsServer.isTextFile("/run/throughput.png")).isFalse();
    }

    private HttpResponse<String> get(String path) throws Exception {
        URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + path);
        return client.send(HttpRequest.newBuilder(uri).build(), HttpResponse.BodyHandlers.ofString());
    }
}
