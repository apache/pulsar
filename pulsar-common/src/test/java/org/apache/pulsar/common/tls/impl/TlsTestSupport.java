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
package org.apache.pulsar.common.tls.impl;

import com.google.common.io.Resources;
import io.opentelemetry.api.OpenTelemetry;
import java.io.File;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import org.apache.pulsar.tls.TlsFactoryInitContext;

/**
 * Shared helpers for the {@code FileBasedTlsFactory} tests: a test {@link TlsFactoryInitContext} and a
 * self-contained in-memory {@link SSLEngine} handshake pump (no sockets, no event loops).
 */
final class TlsTestSupport {

    private TlsTestSupport() {
    }

    static String resource(String name) {
        return new File(Resources.getResource(name).getPath()).getAbsolutePath();
    }

    static TlsFactoryInitContext initContext(ScheduledExecutorService scheduler, Executor blockingExecutor) {
        return initContext(scheduler, blockingExecutor, OpenTelemetry.noop());
    }

    static TlsFactoryInitContext initContext(ScheduledExecutorService scheduler, Executor blockingExecutor,
            OpenTelemetry openTelemetry) {
        return initContext(scheduler, blockingExecutor, openTelemetry, Clock.systemUTC());
    }

    static TlsFactoryInitContext initContext(ScheduledExecutorService scheduler, Executor blockingExecutor,
            OpenTelemetry openTelemetry, Clock clock) {
        return new TlsFactoryInitContext() {
            @Override
            public Map<String, String> params() {
                return Map.of();
            }

            @Override
            public ScheduledExecutorService scheduler() {
                return scheduler;
            }

            @Override
            public Executor blockingExecutor() {
                return blockingExecutor;
            }

            @Override
            public Clock clock() {
                return clock;
            }

            @Override
            public OpenTelemetry openTelemetry() {
                return openTelemetry;
            }
        };
    }

    /**
     * Drive a full in-memory TLS handshake between two engines until both stop handshaking, or throw if
     * it fails (e.g. an untrusted client certificate rejected by a secure server).
     */
    static void handshake(SSLEngine client, SSLEngine server) throws Exception {
        ByteBuffer clientToServer = ByteBuffer.allocate(client.getSession().getPacketBufferSize());
        ByteBuffer serverToClient = ByteBuffer.allocate(server.getSession().getPacketBufferSize());
        ByteBuffer clientApp = ByteBuffer.allocate(client.getSession().getApplicationBufferSize() + 64);
        ByteBuffer serverApp = ByteBuffer.allocate(server.getSession().getApplicationBufferSize() + 64);
        ByteBuffer empty = ByteBuffer.allocate(0);

        client.beginHandshake();
        server.beginHandshake();

        for (int i = 0; i < 200; i++) {
            if (isDone(client) && isDone(server)) {
                return;
            }
            runTasks(client.wrap(empty, clientToServer), client);
            runTasks(server.wrap(empty, serverToClient), server);
            clientToServer.flip();
            serverToClient.flip();
            runTasks(client.unwrap(serverToClient, clientApp), client);
            runTasks(server.unwrap(clientToServer, serverApp), server);
            clientToServer.compact();
            serverToClient.compact();
        }
        throw new IllegalStateException("TLS handshake did not complete within the iteration budget");
    }

    private static boolean isDone(SSLEngine engine) {
        SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();
        return status == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
                || status == SSLEngineResult.HandshakeStatus.FINISHED;
    }

    private static void runTasks(SSLEngineResult result, SSLEngine engine) {
        if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK) {
            Runnable task;
            while ((task = engine.getDelegatedTask()) != null) {
                task.run();
            }
        }
    }
}
