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
package org.apache.pulsar.broker.authentication;

import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.net.ssl.SSLSession;
import lombok.Builder;
import lombok.Getter;
import org.apache.pulsar.common.api.proto.CommandConnect;

/**
 * Context object that encapsulates all information required to perform binary protocol
 * authentication for a client connection.
 * <p>
 * This context is used by {@link BinaryAuthSession} to manage authentication state,
 * credentials, and related connection details during the authentication process.
 */
@Getter
@Builder
public class BinaryAuthContext {
    /**
     * The {@link CommandConnect} object representing the client's connection request.
     */
    private CommandConnect commandConnect;

    /**
     * The {@link SSLSession} associated with the connection, if TLS/SSL is used.
     *
     * <p>May be {@code null} for non-TLS connections. When present, authenticators
     * can use session details (peer certificates, cipher suite, etc.) as part of
     * the authentication decision.
     */
    private SSLSession sslSession;

    /**
     * The {@link AuthenticationService} used to perform authentication operations.
     *
     * <p>This is typically the broker-level service that coordinates available
     * authentication providers and performs lifecycle operations such as
     * verifying credentials or initiating challenges.
     */
    private AuthenticationService authenticationService;

    /**
     * The executor used to perform asynchronous authentication operations.
     * <p>
     * This should be the Netty event loop executor associated with the current connection,
     * ensuring that authentication tasks run on the same event loop thread.
     */
    private Executor executor;

    /**
     * The remote {@link SocketAddress} of the client initiating the connection.
     *
     * <p>This may be used for audit, logging, access control decisions, or for
     * binding authentication state to the client's address.
     */
    private SocketAddress remoteAddress;

    /**
     * Indicates whether to authenticate the client's original authentication data
     * instead of simply trusting the provided principal.
     * <p>
     * When set to {@code true}, the session re-validates the original authentication data
     * sent by the client. When set to {@code false}, it skips re-authentication
     * and only authorizes the provided principal if necessary.
     */
    private boolean authenticateOriginalAuthData;

    /**
     * A supplier that indicates whether the current connection is still in the
     * initial connect phase.
     *
     * <p>When this supplier returns {@code true}, the connection is being
     * established and the broker should treat the incoming data as part of the
     * initial connect handling. When it returns {@code false}, the
     * client is already marked connected; subsequent authentication events
     * represent refreshes or re-authentication.
     *
     * <p>Using a supplier allows deferred evaluation of the initial-connect state
     * (for example, if connection state may change between when the context is
     * created and when authentication is executed).
     */
    private Supplier<Boolean> isInitialConnectSupplier;
}
