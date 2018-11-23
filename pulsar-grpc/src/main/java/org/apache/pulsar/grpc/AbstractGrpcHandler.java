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

import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.grpc.proto.ClientParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.NoPermissionException;
import java.io.Closeable;
import java.net.InetSocketAddress;

import static org.apache.pulsar.grpc.Constants.*;

public abstract class AbstractGrpcHandler implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractGrpcHandler.class);

    protected final GrpcProxyService service;
    protected final TopicName topic;
    protected final ClientParameters clientParameters;
    protected final InetSocketAddress remoteAddress;
    protected final String authenticationRole;
    protected final AuthenticationDataSource authenticationData;


    public AbstractGrpcHandler(GrpcProxyService service) {
        this.service = service;
        this.topic = TopicName.get(CLIENT_PARAMS_CTX_KEY.get().getTopic());
        this.clientParameters = CLIENT_PARAMS_CTX_KEY.get();
        this.remoteAddress = REMOTE_ADDRESS_CTX_KEY.get();
        this.authenticationRole = AUTHENTICATION_ROLE_CTX_KEY.get();
        this.authenticationData = AUTHENTICATION_DATA_CTX_KEY.get();
    }


    protected void checkAuth() throws Exception {
        if (service.isAuthorizationEnabled()) {
            Boolean authorized;
            try {
                authorized = isAuthorized(authenticationRole, authenticationData);
            } catch (Exception e) {
                log.warn("[{}] Got an exception when authorizing WebSocket client {} on topic {} on: {}",
                        remoteAddress, authenticationRole, topic, e.getMessage());
                throw e;
            }
            if (!authorized) {
                log.warn("[{}] WebSocket Client [{}] is not authorized on topic {}",
                        remoteAddress, authenticationRole, topic.toString());
                throw new NoPermissionException("Not authorized");
            }
        }
    }

    protected abstract Boolean isAuthorized(String authRole, AuthenticationDataSource authenticationData) throws Exception;

}
