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

import java.nio.ByteBuffer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import lombok.extern.slf4j.Slf4j;

/**
 * SSLContextValidatorEngine to validate 2 SSlContext.
 */
@Slf4j
public class SSLContextValidatorEngine {
    /**
     * Mode of peer.
     */
    public enum Mode {
        CLIENT,
        SERVER
    }

    private static final ByteBuffer EMPTY_BUF = ByteBuffer.allocate(0);
    private final SSLEngine sslEngine;
    private SSLEngineResult handshakeResult;
    private ByteBuffer appBuffer;
    private ByteBuffer netBuffer;
    private Mode mode;

    public static void validate(SSLContext clientSslContext, SSLContext serverSslContext) throws SSLException {
        SSLContextValidatorEngine clientEngine = new SSLContextValidatorEngine(clientSslContext, Mode.CLIENT);
        SSLContextValidatorEngine serverEngine = new SSLContextValidatorEngine(serverSslContext, Mode.SERVER);
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

    private SSLContextValidatorEngine(SSLContext sslContext, Mode mode) {
        this.mode = mode;
        this.sslEngine = createSslEngine(sslContext, "localhost", 0); // these hints are not used for validation
        sslEngine.setUseClientMode(mode == Mode.CLIENT);
        appBuffer = ByteBuffer.allocate(sslEngine.getSession().getApplicationBufferSize());
        netBuffer = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
    }

    private SSLEngine createSslEngine(SSLContext sslContext, String peerHost, int peerPort) {
        SSLEngine sslEngine = sslContext.createSSLEngine(peerHost, peerPort);

        if (mode == Mode.SERVER) {
            sslEngine.setNeedClientAuth(true);
        } else {
            sslEngine.setUseClientMode(true);
            SSLParameters sslParams = sslEngine.getSSLParameters();
            sslEngine.setSSLParameters(sslParams);
        }
        return sslEngine;
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
                    if (handshakeResult.getHandshakeStatus() != SSLEngineResult.HandshakeStatus.FINISHED) {
                        throw new SSLException("Did not finish handshake");
                    }
                    return;
                default:
                    throw new IllegalStateException("Unexpected handshake status " + handshakeStatus);
            }
        }
    }

    boolean complete() {
        return sslEngine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED
               || sslEngine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING;
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
