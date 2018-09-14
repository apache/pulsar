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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Objects;

import javax.net.ssl.SSLContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.pulsar.broker.web.AuthenticationFilter;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AdminProxyHandler extends AsyncProxyServlet {
    private static final Logger LOG = LoggerFactory.getLogger(AdminProxyHandler.class);

    private final ProxyConfiguration config;
    private final BrokerDiscoveryProvider discoveryProvider;
    private final String brokerWebServiceUrl;
    private final String functionWorkerWebServiceUrl;

    AdminProxyHandler(ProxyConfiguration config, BrokerDiscoveryProvider discoveryProvider) {
        this.config = config;
        this.discoveryProvider = discoveryProvider;
        this.brokerWebServiceUrl = config.isTlsEnabledWithBroker() ? config.getBrokerWebServiceURLTLS()
                : config.getBrokerWebServiceURL();
        this.functionWorkerWebServiceUrl = config.isTlsEnabledWithBroker() ? config.getFunctionWorkerWebServiceURLTLS()
                : config.getFunctionWorkerWebServiceURL();
    }

    @Override
    protected HttpClient createHttpClient() throws ServletException {
        HttpClient client = super.createHttpClient();
        client.setFollowRedirects(true);
        return client;
    }

    @Override
    protected HttpClient newHttpClient() {
        try {
            Authentication auth = AuthenticationFactory.create(
                config.getBrokerClientAuthenticationPlugin(),
                config.getBrokerClientAuthenticationParameters()
            );

            Objects.requireNonNull(auth, "No supported auth found for proxy");

            auth.start();

            if (config.isTlsEnabledWithBroker()) {
                try {
                    X509Certificate trustCertificates[] = SecurityUtility
                        .loadCertificatesFromPemFile(config.getTlsTrustCertsFilePath());

                    SSLContext sslCtx;
                    AuthenticationDataProvider authData = auth.getAuthData();
                    if (authData.hasDataForTls()) {
                        sslCtx = SecurityUtility.createSslContext(
                            config.isTlsAllowInsecureConnection(),
                            trustCertificates,
                            authData.getTlsCertificates(),
                            authData.getTlsPrivateKey()
                        );
                    } else {
                        sslCtx = SecurityUtility.createSslContext(
                            config.isTlsAllowInsecureConnection(),
                            trustCertificates
                        );
                    }

                    SslContextFactory contextFactory = new SslContextFactory();
                    contextFactory.setSslContext(sslCtx);

                    return new HttpClient(contextFactory);
                } catch (Exception e) {
                    try {
                        auth.close();
                    } catch (IOException ioe) {
                        LOG.error("Failed to close the authentication service", ioe);
                    }
                    throw new PulsarClientException.InvalidConfigurationException(e.getMessage());
                }
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        // return an unauthenticated client, every request will fail.
        return new HttpClient();
    }

    @Override
    protected String rewriteTarget(HttpServletRequest request) {
        StringBuilder url = new StringBuilder();

        boolean isFunctionsRestRequest = false;
        String requestUri = request.getRequestURI();
        if (requestUri.startsWith("/admin/v2/functions")
            || requestUri.startsWith("/admin/functions")) {
            isFunctionsRestRequest = true;
        }

        if (isFunctionsRestRequest && !isBlank(functionWorkerWebServiceUrl)) {
            url.append(functionWorkerWebServiceUrl);
        } else if (isBlank(brokerWebServiceUrl)) {
            try {
                ServiceLookupData availableBroker = discoveryProvider.nextBroker();

                if (config.isTlsEnabledWithBroker()) {
                    url.append(availableBroker.getWebServiceUrlTls());
                } else {
                    url.append(availableBroker.getWebServiceUrl());
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}:{}] Selected active broker is {}", request.getRemoteAddr(), request.getRemotePort(),
                            url.toString());
                }
            } catch (Exception e) {
                LOG.warn("[{}:{}] Failed to get next active broker {}", request.getRemoteAddr(),
                        request.getRemotePort(), e.getMessage(), e);
                return null;
            }
        } else {
            url.append(brokerWebServiceUrl);
        }

        if (url.lastIndexOf("/") == url.length() - 1) {
            url.deleteCharAt(url.lastIndexOf("/"));
        }
        url.append(requestUri);

        String query = request.getQueryString();
        if (query != null) {
            url.append("?").append(query);
        }

        URI rewrittenUrl = URI.create(url.toString()).normalize();

        if (!validateDestination(rewrittenUrl.getHost(), rewrittenUrl.getPort())) {
            return null;
        }

        return rewrittenUrl.toString();
    }

    @Override
    protected void addProxyHeaders(HttpServletRequest clientRequest, Request proxyRequest) {
        super.addProxyHeaders(clientRequest, proxyRequest);
        String user = (String) clientRequest.getAttribute(AuthenticationFilter.AuthenticatedRoleAttributeName);
        if (user != null) {
            proxyRequest.header("X-Original-Principal", user);
        }
    }
}
