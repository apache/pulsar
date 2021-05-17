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
package org.apache.pulsar.common.util.keystoretls;

import static javax.net.ssl.SSLEngineResult.HandshakeStatus.FINISHED;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;

/**
 * SSLContextValidatorEngine to validate 2 SSlContext.
 */
@Slf4j
public class SSLContextValidatorEngine {
    @FunctionalInterface
    public interface SSLEngineProvider {
        SSLEngine createSSLEngine(String peerHost, int peerPort);
    }

    private static final ByteBuffer EMPTY_BUF = ByteBuffer.allocate(0);
    private final SSLEngine sslEngine;
    private SSLEngineResult handshakeResult;
    private ByteBuffer appBuffer;
    private ByteBuffer netBuffer;
    private boolean finished = false;

    /**
     * Validates TLS handshake up to TLSv1.2.
     * TLSv1.3 has a differences in TLS handshake as described in https://stackoverflow.com/a/62465859
     */
    public static void validate(SSLEngineProvider clientSslEngineSupplier, SSLEngineProvider serverSslEngineSupplier)
            throws SSLException {
        SSLContextValidatorEngine clientEngine = new SSLContextValidatorEngine(clientSslEngineSupplier);
        if (Arrays.stream(clientEngine.sslEngine.getEnabledProtocols()).anyMatch(s -> s.equals("TLSv1.3"))) {
            throw new IllegalStateException("This validator doesn't support TLSv1.3");
        }
        SSLContextValidatorEngine serverEngine = new SSLContextValidatorEngine(serverSslEngineSupplier);
        try {
            clientEngine.beginHandshake();
            serverEngine.beginHandshake();
            while (!serverEngine.complete() || !clientEngine.complete()) {
                clientEngine.handshake(serverEngine);
                serverEngine.handshake(clientEngine);
            }
        } finally {
            clientEngine.close();
            serverEngine.close();
        }
    }

    private SSLContextValidatorEngine(SSLEngineProvider sslEngineSupplier) {
        this.sslEngine = sslEngineSupplier.createSSLEngine("localhost", 0);
        appBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        netBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
    }

    void beginHandshake() throws SSLException {
        sslEngine.beginHandshake();
    }

    void handshake(SSLContextValidatorEngine peerEngine) throws SSLException {
        SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
        while (true) {
            switch (handshakeStatus) {
                case NEED_WRAP:
                    handshakeResult = sslEngine.wrap(EMPTY_BUF, netBuffer);
                    switch (handshakeResult.getStatus()) {
                        case OK: break;
                        case BUFFER_OVERFLOW:
                            netBuffer.compact();
                            netBuffer = ensureCapacity(netBuffer, sslEngine.getSession().getPacketBufferSize());
                            netBuffer.flip();
                            break;
                        case BUFFER_UNDERFLOW:
                        case CLOSED:
                        default:
                            throw new SSLException("Unexpected handshake status: " + handshakeResult.getStatus());
                    }
                    return;
                case NEED_UNWRAP:
                    if (peerEngine.netBuffer.position() == 0) {
                        return;
                    }
                    peerEngine.netBuffer.flip(); // unwrap the data from peer
                    handshakeResult = sslEngine.unwrap(peerEngine.netBuffer, appBuffer);
                    peerEngine.netBuffer.compact();
                    handshakeStatus = handshakeResult.getHandshakeStatus();
                    switch (handshakeResult.getStatus()) {
                        case OK: break;
                        case BUFFER_OVERFLOW:
                            appBuffer = ensureCapacity(appBuffer, sslEngine.getSession().getApplicationBufferSize());
                            break;
                        case BUFFER_UNDERFLOW:
                            netBuffer = ensureCapacity(netBuffer, sslEngine.getSession().getPacketBufferSize());
                            break;
                        case CLOSED:
                        default:
                            throw new SSLException("Unexpected handshake status: " + handshakeResult.getStatus());
                    }
                    break;
                case NEED_TASK:
                    sslEngine.getDelegatedTask().run();
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;
                case FINISHED:
                    return;
                case NOT_HANDSHAKING:
                    if (handshakeResult.getHandshakeStatus() != FINISHED) {
                        throw new SSLException("Did not finish handshake");
                    }
                    finished = true;
                    return;
                default:
                    throw new IllegalStateException("Unexpected handshake status " + handshakeStatus);
            }
        }
    }

    boolean complete() {
        return finished;
    }

    void close() {
        sslEngine.closeOutbound();
        try {
            sslEngine.closeInbound();
        } catch (Exception e) {
            // ignore
        }
    }

    /**
     * Check if the given ByteBuffer capacity.
     * @param existingBuffer ByteBuffer capacity to check
     * @param newLength new length for the ByteBuffer.
     * returns ByteBuffer
     */
    public static ByteBuffer ensureCapacity(ByteBuffer existingBuffer, int newLength) {
        if (newLength > existingBuffer.capacity()) {
            ByteBuffer newBuffer = ByteBuffer.allocate(newLength);
            existingBuffer.flip();
            newBuffer.put(existingBuffer);
            return newBuffer;
        }
        return existingBuffer;
    }
}
