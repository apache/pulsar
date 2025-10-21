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

@Getter
@Builder
public class BinaryAuthContext {
    /**
     * The CommandConnect object representing the client's connection request.
     */
    private CommandConnect commandConnect;

    /**
     * The SSLSession associated with the connection, if SSL/TLS is used.
     */
    private SSLSession sslSession;

    /**
     * The AuthenticationService used to perform authentication for this context.
     */
    private AuthenticationService authenticationService;

    /**
     * The Executor to use for asynchronous authentication operations.
     * Must be provided if authentication involves async tasks.
     */
    private Executor executor;

    /**
     * The remote address of the client initiating the connection.
     */
    private SocketAddress remoteAddress;

    /**
     * If true, authentication should be performed using the original authentication data
     * provided by the client, rather than any intermediate or proxy data.
     * Set to true when authenticating the initial client request.
     */
    private boolean authenticateOriginalAuthData;

    /**
     * Supplier indicating whether the connection is currently in the process of connecting.
     * Used to determine connection state during authentication.
     */
    private Supplier<Boolean> isConnectingSupplier;
}
