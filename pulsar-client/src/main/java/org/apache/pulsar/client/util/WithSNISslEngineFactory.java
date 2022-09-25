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
package org.apache.pulsar.client.util;

import java.util.Collections;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.netty.ssl.DefaultSslEngineFactory;

public class WithSNISslEngineFactory extends DefaultSslEngineFactory {
    private final String host;

    public WithSNISslEngineFactory(String host) {
        this.host = host;
    }

    @Override
    protected void configureSslEngine(SSLEngine sslEngine, AsyncHttpClientConfig config) {
        super.configureSslEngine(sslEngine, config);
        SSLParameters params = sslEngine.getSSLParameters();
        params.setServerNames(Collections.singletonList(new SNIHostName(host)));
        sslEngine.setSSLParameters(params);
    }
}
