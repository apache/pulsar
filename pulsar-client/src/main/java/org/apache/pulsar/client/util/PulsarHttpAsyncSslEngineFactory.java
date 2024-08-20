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
package org.apache.pulsar.client.util;

import java.util.Collections;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.PulsarSslFactory;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.netty.ssl.DefaultSslEngineFactory;

public class PulsarHttpAsyncSslEngineFactory extends DefaultSslEngineFactory {

    private final PulsarSslFactory pulsarSslFactory;
    private final String host;

    public PulsarHttpAsyncSslEngineFactory(PulsarSslFactory pulsarSslFactory, String host) {
        this.pulsarSslFactory = pulsarSslFactory;
        this.host = host;
    }

    @Override
    protected void configureSslEngine(SSLEngine sslEngine, AsyncHttpClientConfig config) {
        super.configureSslEngine(sslEngine, config);
        if (StringUtils.isNotBlank(host)) {
            SSLParameters parameters = sslEngine.getSSLParameters();
            parameters.setServerNames(Collections.singletonList(new SNIHostName(host)));
            sslEngine.setSSLParameters(parameters);
        }
    }

    @Override
    public SSLEngine newSslEngine(AsyncHttpClientConfig config, String peerHost, int peerPort) {
        SSLContext sslContext = this.pulsarSslFactory.getInternalSslContext();
        SSLEngine sslEngine = config.isDisableHttpsEndpointIdentificationAlgorithm()
                ? sslContext.createSSLEngine() :
                sslContext.createSSLEngine(domain(peerHost), peerPort);
        configureSslEngine(sslEngine, config);
        return  sslEngine;
    }

}