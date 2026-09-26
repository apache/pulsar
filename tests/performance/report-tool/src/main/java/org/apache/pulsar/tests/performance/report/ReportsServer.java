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

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.SimpleFileServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Serves the performance reports root over HTTP with the JDK's {@link SimpleFileServer}. Its directory listings lead
 * through days, branches and names to the runs, and a run's directory opens on its {@code index.html}, the run
 * report. The text files a run writes, such as YAML, CSV, HDR latency logs, collapsed stacks, logs and Markdown, are
 * served as plain text, so that browsers show them instead of offering them as downloads; JFR recordings and capture
 * streams stay downloads. The text file extensions are the same as in {@code tests/performance/serve-reports.py}.
 */
@Command(name = "serve-reports", mixinStandardHelpOptions = true,
        description = "Serve the performance reports over HTTP, with the text files a run writes shown as text")
public final class ReportsServer implements Callable<Integer> {
    static final String TEXT = "text/plain; charset=utf-8";
    static final List<String> TEXT_EXTENSIONS =
            List.of(".yaml", ".yml", ".csv", ".hdr", ".hgrm", ".collapsed", ".log", ".md", ".txt");

    @Option(names = "--directory", required = true, description = "The reports root to serve")
    private Path directory;

    @Option(names = "--address", defaultValue = "127.0.0.1",
            description = "The address to bind to; default: ${DEFAULT-VALUE}, reachable only from this host")
    private String address;

    @Option(names = "--port", defaultValue = "8000", description = "The port to listen on; default: ${DEFAULT-VALUE}")
    private int port;

    private ReportsServer() {
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new ReportsServer()).execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    @Override
    public Integer call() throws Exception {
        Path root = directory.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException(root + " is not a directory");
        }
        HttpServer server = start(root, new InetSocketAddress(address, port), System.out);
        System.out.println("Serving " + root + " at http://" + address + ":" + server.getAddress().getPort() + "/");
        // The server's dispatcher thread isn't a daemon thread, so it runs until the process is stopped
        return 0;
    }

    /** Starts serving {@code root}, logging every request to {@code log}. */
    static HttpServer start(Path root, InetSocketAddress socketAddress, OutputStream log) throws IOException {
        HttpServer server = HttpServer.create(socketAddress, 0, "/", SimpleFileServer.createFileHandler(root),
                SimpleFileServer.createOutputFilter(log, SimpleFileServer.OutputLevel.INFO),
                textFilesAsPlainText());
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        return server;
    }

    static boolean isTextFile(String path) {
        String lowerCase = path.toLowerCase(Locale.ROOT);
        return TEXT_EXTENSIONS.stream().anyMatch(lowerCase::endsWith);
    }

    /**
     * Sets the content type of the text files to {@link #TEXT} just before the file handler sends the response
     * headers. The file handler still decides what is served, including its checks of the requested path.
     */
    static Filter textFilesAsPlainText() {
        return new Filter() {
            @Override
            public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
                chain.doFilter(isTextFile(exchange.getRequestURI().getPath())
                        ? new PlainTextExchange(exchange) : exchange);
            }

            @Override
            public String description() {
                return "Serves the text files of a run as " + TEXT;
            }
        };
    }

    /** Delegates to an exchange, replacing the content type of a successful response with {@link #TEXT}. */
    private static final class PlainTextExchange extends HttpExchange {
        private final HttpExchange exchange;

        PlainTextExchange(HttpExchange exchange) {
            this.exchange = exchange;
        }

        @Override
        public void sendResponseHeaders(int responseCode, long responseLength) throws IOException {
            if (responseCode == 200) {
                exchange.getResponseHeaders().set("Content-Type", TEXT);
            }
            exchange.sendResponseHeaders(responseCode, responseLength);
        }

        @Override
        public Headers getRequestHeaders() {
            return exchange.getRequestHeaders();
        }

        @Override
        public Headers getResponseHeaders() {
            return exchange.getResponseHeaders();
        }

        @Override
        public URI getRequestURI() {
            return exchange.getRequestURI();
        }

        @Override
        public String getRequestMethod() {
            return exchange.getRequestMethod();
        }

        @Override
        public HttpContext getHttpContext() {
            return exchange.getHttpContext();
        }

        @Override
        public void close() {
            exchange.close();
        }

        @Override
        public InputStream getRequestBody() {
            return exchange.getRequestBody();
        }

        @Override
        public OutputStream getResponseBody() {
            return exchange.getResponseBody();
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return exchange.getRemoteAddress();
        }

        @Override
        public int getResponseCode() {
            return exchange.getResponseCode();
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return exchange.getLocalAddress();
        }

        @Override
        public String getProtocol() {
            return exchange.getProtocol();
        }

        @Override
        public Object getAttribute(String name) {
            return exchange.getAttribute(name);
        }

        @Override
        public void setAttribute(String name, Object value) {
            exchange.setAttribute(name, value);
        }

        @Override
        public void setStreams(InputStream input, OutputStream output) {
            exchange.setStreams(input, output);
        }

        @Override
        public HttpPrincipal getPrincipal() {
            return exchange.getPrincipal();
        }
    }
}
