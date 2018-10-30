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
package org.apache.pulsar.proxy.server;

import static org.apache.pulsar.client.impl.HttpClient.getPulsarClientVersion;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;

public class ProxyClientCnx extends ClientCnx {

    String clientAuthRole;
    String clientAuthData;
    String clientAuthMethod;
    int protocolVersion;

    public ProxyClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup, String clientAuthRole,
            String clientAuthData, String clientAuthMethod, int protocolVersion) {
        super(conf, eventLoopGroup);
        this.clientAuthRole = clientAuthRole;
        this.clientAuthData = clientAuthData;
        this.clientAuthMethod = clientAuthMethod;
        this.protocolVersion = protocolVersion;
    }

    @Override
    protected ByteBuf newConnectCommand() throws PulsarClientException {
        if (log.isDebugEnabled()) {
            log.debug(
                    "New Connection opened via ProxyClientCnx with params clientAuthRole = {}, clientAuthData = {}, clientAuthMethod = {}",
                    clientAuthRole, clientAuthData, clientAuthMethod);
        }
        String authData = null;
        if (authentication.getAuthData().hasDataFromCommand()) {
            authData = authentication.getAuthData().getCommandData();
        }
        return Commands.newConnect(authentication.getAuthMethodName(), authData, protocolVersion,
                getPulsarClientVersion(), proxyToTargetBrokerAddress, clientAuthRole, clientAuthData, clientAuthMethod);
    }

    private static final Logger log = LoggerFactory.getLogger(ProxyClientCnx.class);
}
