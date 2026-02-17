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
package org.apache.pulsar.proxy.server;

import com.google.common.base.Strings;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLRegexLookupProxyHandler extends DefaultLookupProxyHandler {

    private static final Logger log = LoggerFactory.getLogger(URLRegexLookupProxyHandler.class);

    private Pattern pattern;

    private String replacement;

    @Override
    public void initialize(ProxyService proxy, ProxyConnection proxyConnection) {
        super.initialize(proxy, proxyConnection);
        Properties properties = proxy.getConfiguration().getProperties();
        String regex = properties.getProperty("urlRegexLookupProxyHandlerRegex");
        if (Strings.isNullOrEmpty(regex)) {
            throw new IllegalArgumentException("urlRegexLookupProxyHandlerRegex is not set");
        }
        this.pattern = Pattern.compile(regex);
        this.replacement = properties.getProperty("urlRegexLookupProxyHandlerReplacement");
        if (Strings.isNullOrEmpty(this.replacement)) {
            throw new IllegalArgumentException("urlRegexLookupProxyHandlerReplacement is not set");
        }
    }

    @Override
    public void handleLookup(CommandLookupTopic lookup) {
        if (log.isDebugEnabled()) {
            log.debug("Received Lookup from {}", clientAddress);
        }
        long clientRequestId = lookup.getRequestId();
        if (lookupRequestSemaphore.tryAcquire()) {
            try {
                LOOKUP_REQUESTS.inc();
                String serviceUrl = getBrokerServiceUrl(clientRequestId);
                if (serviceUrl != null) {
                    if (lookup.isAuthoritative()) {
                        performLookup(clientRequestId, lookup.getTopic(), serviceUrl, false, 10)
                            .whenComplete(
                                (brokerUrl, ex) -> {
                                    if (ex != null) {
                                        ServerError serverError = ex instanceof LookupException
                                            ? ((LookupException) ex).getServerError()
                                            : getServerError(ex);
                                        proxyConnection.ctx().writeAndFlush(
                                            Commands.newLookupErrorResponse(serverError, ex.getMessage(),
                                                clientRequestId));
                                    } else {
                                        proxyConnection.ctx().writeAndFlush(
                                            Commands.newLookupResponse(brokerUrl, brokerUrl, true,
                                                CommandLookupTopicResponse.LookupType.Connect, clientRequestId,
                                                true /* this is coming from proxy */));
                                    }
                                });
                    } else {
                        performLookup(clientRequestId, lookup.getTopic(), serviceUrl, false, 10)
                            .whenComplete(
                                (brokerUrl, ex) -> {
                                    try {
                                        if (pattern.matcher(brokerUrl).matches()) {
                                            if (log.isDebugEnabled()) {
                                                log.debug("Broker URL {} matches regex {}", brokerUrl, pattern);
                                            }
                                            String proxyUrl = pattern.matcher(brokerUrl).replaceAll(replacement);
                                            if (log.isDebugEnabled()) {
                                                log.debug("Redirect to proxy URL {}", proxyUrl);
                                            }
                                            proxyConnection.ctx().writeAndFlush(
                                                Commands.newLookupResponse(proxyUrl, proxyUrl, true,
                                                    CommandLookupTopicResponse.LookupType.Redirect, clientRequestId,
                                                    false));
                                        } else {
                                            if (log.isDebugEnabled()) {
                                                log.debug("Broker URL {} doesn't match regex {}", brokerUrl, pattern);
                                            }
                                            proxyConnection.ctx().writeAndFlush(
                                                Commands.newLookupErrorResponse(ServerError.ServiceNotReady,
                                                    "Broker URL does not match the lookup handler regex",
                                                    clientRequestId));
                                        }
                                    } catch (IllegalArgumentException iae) {
                                        proxyConnection.ctx().writeAndFlush(
                                            Commands.newLookupErrorResponse(ServerError.ServiceNotReady,
                                                iae.getMessage(), clientRequestId));
                                    }
                                });

                    }
                }
            } finally {
                lookupRequestSemaphore.release();
            }
        } else {
            REJECTED_LOOKUP_REQUESTS.inc();
            if (log.isDebugEnabled()) {
                log.debug("Lookup Request ID {} from {} rejected - {}.", clientRequestId, clientAddress,
                    throttlingErrorMessage);
            }
            proxyConnection.ctx().writeAndFlush(Commands.newLookupErrorResponse(ServerError.ServiceNotReady,
                throttlingErrorMessage, clientRequestId));
        }
    }
}
