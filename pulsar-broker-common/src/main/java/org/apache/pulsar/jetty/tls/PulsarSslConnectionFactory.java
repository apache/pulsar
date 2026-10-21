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
package org.apache.pulsar.jetty.tls;

import com.google.common.annotations.VisibleForTesting;
import javax.net.ssl.SSLEngine;
import lombok.CustomLog;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.io.ssl.SslConnection;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * A Jetty {@link SslConnectionFactory} that closes a Conscrypt {@link SSLEngine} when its connection closes.
 *
 * <p>Jetty's {@link SslConnection} closes the engine's inbound side only when the peer sends a TLS
 * {@code close_notify}. A client that drops the TCP connection without one leaves the engine open, and Conscrypt
 * then releases the native {@code SSL} object only when the engine is finalized. For an engine that was not
 * closed, {@code ConscryptEngine.finalize()} reads the native session, while {@code NativeSsl.finalize()} frees the
 * same native object. The two finalizers use different locks, so when they run on two threads at once (as they do
 * with {@code System.runFinalization()}), the read can reach freed memory and crash the JVM with SIGSEGV.
 *
 * <p>Closing both sides when the connection closes moves the engine to its closed state on the thread that closed
 * the connection, while the native {@code SSL} object is still alive. The finalizer then skips the session read.
 * Only Conscrypt engines are closed this way: other providers, such as SunJSSE, treat a {@code closeInbound()}
 * without the peer's {@code close_notify} as a fatal error and invalidate the session.
 */
@CustomLog
public class PulsarSslConnectionFactory extends SslConnectionFactory {
    private static final Connection.Listener CLOSE_CONSCRYPT_ENGINE_ON_CLOSE = new Connection.Listener() {
        @Override
        public void onClosed(Connection connection) {
            if (connection instanceof SslConnection sslConnection) {
                closeConscryptEngine(sslConnection.getSSLEngine());
            }
        }
    };

    public PulsarSslConnectionFactory(SslContextFactory.Server factory, String nextProtocol) {
        super(factory, nextProtocol);
    }

    @Override
    protected SslConnection newSslConnection(Connector connector, EndPoint endPoint, SSLEngine engine) {
        SslConnection sslConnection = super.newSslConnection(connector, endPoint, engine);
        if (isConscryptEngine(engine)) {
            sslConnection.addEventListener(CLOSE_CONSCRYPT_ENGINE_ON_CLOSE);
        }
        return sslConnection;
    }

    @VisibleForTesting
    static boolean isConscryptEngine(SSLEngine engine) {
        return engine.getClass().getName().startsWith("org.conscrypt.");
    }

    @VisibleForTesting
    static void closeConscryptEngine(SSLEngine engine) {
        try {
            engine.closeOutbound();
            engine.closeInbound();
        } catch (Exception e) {
            log.debug().exception(e).log("Failed to close the SSL engine of a closed connection");
        }
    }
}
