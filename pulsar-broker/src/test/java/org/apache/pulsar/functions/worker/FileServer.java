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
package org.apache.pulsar.functions.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;

/**
 * Simple http server for serving files in Pulsar Function test cases
 */
@Slf4j
public class FileServer implements AutoCloseable {
    private static final String HEALTH_PATH = "/health";
    private final HttpServer httpServer;

    public FileServer() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        // creates a default executor
        httpServer.setExecutor(null);
        httpServer.createContext(HEALTH_PATH, he -> {
           he.sendResponseHeaders(204, 0);
        });
    }

    public void serveFile(String path, File file) {
        assertTrue(file.exists(), file.getAbsolutePath() + " doesn't exist.");
        httpServer.createContext(path, he -> {
            try {
                Headers headers = he.getResponseHeaders();
                headers.add("Content-Type", "application/octet-stream");

                he.sendResponseHeaders(200, file.length());
                try (OutputStream outputStream = he.getResponseBody()) {
                    Files.copy(file.toPath(), outputStream);
                }
            } catch (Exception e) {
                log.error("Error serving file {} for path {}", file, path, e);
            }
        });
    }

    public void start() {
        httpServer.start();
        waitUntilServerIsAvailable();
    }

    private void waitUntilServerIsAvailable() {
        // wait until server is available.
        // There has been a few flakiness issues where the server hasn't been available when
        // the system-under-test has started to download files
        // this assertion will call the "/health" endpoint and check that 204 status code is returned.
        Awaitility.await()
                .ignoreExceptions()
                .untilAsserted(() -> {
                    HttpURLConnection urlConnection = (HttpURLConnection) new URL(getUrl(HEALTH_PATH))
                            .openConnection();
                    urlConnection.setUseCaches(false);
                    urlConnection.setConnectTimeout(5000);
                    urlConnection.setReadTimeout(5000);
                    try {
                        urlConnection.connect();
                        assertEquals(urlConnection.getResponseCode(), 204);
                    } finally {
                        urlConnection.disconnect();
                    }
                });
    }

    public void stop() {
        httpServer.stop(0);
    }

    public String getUrl(String path) {
        return "http://127.0.0.1:" + httpServer.getAddress().getPort() + path;
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
