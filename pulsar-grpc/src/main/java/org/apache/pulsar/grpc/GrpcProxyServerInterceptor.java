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

import io.grpc.*;
import org.apache.pulsar.grpc.proto.ClientParameters;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.pulsar.grpc.Constants.*;

public class GrpcProxyServerInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> serverCall, Metadata metadata, ServerCallHandler<ReqT, RespT> serverCallHandler) {
        Context ctx = Context.current();

        try {
            ClientParameters params = ClientParameters.parseFrom(metadata.get(CLIENT_PARAMS_METADATA_KEY));
            checkArgument(!params.getTopic().isEmpty(), "Empty topic name");
            ctx = ctx.withValue(CLIENT_PARAMS_CTX_KEY, params);
        } catch (Exception e) {
            throw Status.INVALID_ARGUMENT.withDescription("Invalid stream metadata: " + e.getMessage()).asRuntimeException(metadata);
        }

        SocketAddress socketAddress = serverCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
        if(socketAddress instanceof InetSocketAddress) {
            ctx = ctx.withValue(REMOTE_ADDRESS_CTX_KEY, (InetSocketAddress)socketAddress);
        }

        return Contexts.interceptCall(ctx, serverCall, metadata, serverCallHandler);
    }

}
