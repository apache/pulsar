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
package org.apache.pulsar.tests.performance.tools;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Lets the launcher control when the producer's measurement starts, over HTTP with the JDK's built-in server: the
 * launcher reaches the producer container's control port through the port that Testcontainers maps on the host.
 * The producer signals that every warmup round has been received, and the launcher, for example once the host has
 * cooled down, starts the measurement:
 *
 * <ul>
 *   <li>{@code GET /measurement/ready?waitMillis=<ms>} waits up to that long for the producer to be ready, and
 *   answers 200 when it is and 204 when it isn't yet, so that the launcher learns it at once without polling
 *   files;</li>
 *   <li>{@code POST /measurement/start} starts the measurement.</li>
 * </ul>
 */
final class MeasurementControl implements AutoCloseable {
    static final String READY_PATH = "/measurement/ready";
    static final String START_PATH = "/measurement/start";
    // The JDK's server doesn't time out a request that is being answered (sun.net.httpserver.maxReqTime and
    // maxRspTime default to none); it closes connections left idle for sun.net.httpserver.idleInterval, 30 s by
    // default. A ready request waits at most this long, well within that, and the launcher asks again.
    static final long MAX_WAIT_MILLIS = TimeUnit.SECONDS.toMillis(10);

    private final HttpServer server;
    private final ExecutorService executor;
    private final CountDownLatch ready = new CountDownLatch(1);
    private final CountDownLatch start = new CountDownLatch(1);

    private MeasurementControl(HttpServer server, ExecutorService executor) {
        this.server = server;
        this.executor = executor;
    }

    /** Serves the control endpoints on {@code port} of every interface; 0 picks a free port. */
    static MeasurementControl start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        // A waiting ready request holds its thread, so that it must not hold up a start request
        ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(runnable, "measurement-control");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        MeasurementControl control = new MeasurementControl(server, executor);
        server.createContext(READY_PATH, control::handleReady);
        server.createContext(START_PATH, control::handleStart);
        server.start();
        return control;
    }

    /** The port the server listens on. */
    int port() {
        return server.getAddress().getPort();
    }

    /** Tells the launcher that the producer is about to send its first measured message. */
    void markReady() {
        ready.countDown();
    }

    /** Waits until the launcher starts the measurement. */
    void awaitStart(long deadlineNanos) throws InterruptedException {
        if (!start.await(Math.max(0, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS)) {
            throw new IllegalStateException("Timed out waiting for the launcher to start the measurement");
        }
    }

    private void handleReady(HttpExchange exchange) throws IOException {
        long waitMillis = 0;
        String query = exchange.getRequestURI().getQuery();
        if (query != null && query.startsWith("waitMillis=")) {
            try {
                waitMillis = Math.min(MAX_WAIT_MILLIS, Long.parseLong(query.substring("waitMillis=".length())));
            } catch (NumberFormatException e) {
                respond(exchange, 400, "waitMillis must be a number of milliseconds\n");
                return;
            }
        }
        boolean isReady;
        try {
            isReady = ready.await(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            isReady = false;
        }
        if (isReady) {
            respond(exchange, 200, "ready\n");
        } else {
            respond(exchange, 204, null);
        }
    }

    private void handleStart(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            respond(exchange, 405, "POST starts the measurement\n");
            return;
        }
        start.countDown();
        respond(exchange, 200, "started\n");
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        if (body == null) {
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
            return;
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
