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
package org.apache.pulsar.client.impl.http;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.resolver.NameResolver;
import io.netty.util.Timer;
import java.io.File;
import java.net.InetAddress;
import java.util.Optional;
import javax.net.ssl.SSLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.AuthenticationInitContext;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

@Slf4j
public class AuthenticationHttpClientFactory {

    private final AuthenticationHttpClientConfig config;
    private final AuthenticationInitContext context;

    public AuthenticationHttpClientFactory(AuthenticationHttpClientConfig config,
                                           AuthenticationInitContext context) {
        this.config = config;
        this.context = context;
    }

    public AsyncHttpClient createHttpClient() {
        DefaultAsyncHttpClientConfig.Builder confBuilder = buildBaseConfig();
        return new DefaultAsyncHttpClient(confBuilder.build());
    }

    @SuppressWarnings("unchecked")
    public NameResolver<InetAddress> getNameResolver() {
        return Optional.ofNullable(context)
                .flatMap(ctx -> ctx.getService(NameResolver.class))
                .orElse(null);
    }

    private DefaultAsyncHttpClientConfig.Builder buildBaseConfig() {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();

        if (context != null) {
            context.getService(Timer.class).ifPresent(confBuilder::setNettyTimer);
            context.getService(EventLoopGroup.class).ifPresent(confBuilder::setEventLoopGroup);
        }

        confBuilder.setCookieStore(null);
        confBuilder.setUseProxyProperties(true);
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout(config.getConnectTimeout());
        confBuilder.setReadTimeout(config.getReadTimeout());
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));

        if (StringUtils.isNotBlank(config.getTrustCertsFilePath())) {
            try {
                confBuilder.setSslContext(SslContextBuilder.forClient()
                        .trustManager(new File(config.getTrustCertsFilePath()))
                        .build());
            } catch (SSLException e) {
                log.error("Could not set trustCertsFilePath", e);
            }
        }

        return confBuilder;
    }
}