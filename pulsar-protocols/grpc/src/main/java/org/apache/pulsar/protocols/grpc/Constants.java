package org.apache.pulsar.protocols.grpc;

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
import io.grpc.Context;
import io.grpc.Metadata;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.protocols.grpc.api.CommandProducer;

import java.net.SocketAddress;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static io.grpc.Metadata.BINARY_BYTE_MARSHALLER;

public class Constants {
    public static final Metadata.Key<byte[]> PRODUCER_PARAMS_METADATA_KEY = Metadata.Key.of("pulsar-producer-params-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<String> ERROR_CODE_METADATA_KEY = Metadata.Key.of("pulsar-error-code", ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<byte[]> AUTH_METADATA_KEY = Metadata.Key.of("pulsar-auth-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<byte[]> AUTHCHALLENGE_METADATA_KEY = Metadata.Key.of("pulsar-authchallenge-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<byte[]> AUTHRESPONSE_METADATA_KEY = Metadata.Key.of("pulsar-authresponse-bin", BINARY_BYTE_MARSHALLER);
    public static final Metadata.Key<byte[]> AUTH_ROLE_TOKEN_METADATA_KEY = Metadata.Key.of("pulsar-authroletoken-bin", BINARY_BYTE_MARSHALLER);

    public static final Context.Key<CommandProducer> PRODUCER_PARAMS_CTX_KEY = Context.key("ProducerParams");
    public static final Context.Key<SocketAddress> REMOTE_ADDRESS_CTX_KEY = Context.key("RemoteAddress");
    public static final Context.Key<String> AUTH_ROLE_CTX_KEY = Context.key("AuthRole");
    public static final Context.Key<AuthenticationDataSource> AUTH_DATA_CTX_KEY = Context.key("AuthenticationData");

    public static final String GRPC_SERVICE_HOST_PROPERTY_NAME = "grpcServiceHost";
    public static final String GRPC_SERVICE_PORT_PROPERTY_NAME = "grpcServicePort";
    public static final String GRPC_SERVICE_PORT_TLS_PROPERTY_NAME = "grpcServicePortTls";

}
