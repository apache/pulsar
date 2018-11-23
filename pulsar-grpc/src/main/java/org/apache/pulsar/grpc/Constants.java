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
package org.apache.pulsar.grpc;

import io.grpc.Context;
import io.grpc.Metadata;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.grpc.proto.ClientParameters;

import java.net.InetSocketAddress;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static io.grpc.Metadata.BINARY_BYTE_MARSHALLER;

public class Constants {

    public static final Metadata.Key<byte[]> CLIENT_PARAMS_METADATA_KEY = Metadata.Key.of("pulsar-client-params-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY = Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> ATHENZ_METADATA_KEY = Metadata.Key.of("Athenz-Role-Auth", ASCII_STRING_MARSHALLER);

    public static final Context.Key<ClientParameters> CLIENT_PARAMS_CTX_KEY = Context.key("ClientParams");
    public static final Context.Key<String> AUTHENTICATION_ROLE_CTX_KEY = Context.key("Authentication-role");
    public static final Context.Key<AuthenticationDataSource> AUTHENTICATION_DATA_CTX_KEY = Context.key("Authentication-data");
    public static final Context.Key<InetSocketAddress> REMOTE_ADDRESS_CTX_KEY = Context.key("RemoteAddress");
}
