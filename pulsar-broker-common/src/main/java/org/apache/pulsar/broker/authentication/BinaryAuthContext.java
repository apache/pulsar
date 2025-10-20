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
    private CommandConnect commandConnect;
    private SSLSession sslSession;
    private AuthenticationService authenticationService;
    private Executor executor;
    private SocketAddress remoteAddress;
    private boolean authenticateOriginalAuthData;
    private Supplier<Boolean> isConnectingSupplier;
}
