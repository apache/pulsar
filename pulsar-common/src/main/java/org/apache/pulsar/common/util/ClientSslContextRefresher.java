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
package org.apache.pulsar.common.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslContext;

public class ClientSslContextRefresher extends SslContextRefresher {
    private volatile SslContext sslContext;
    private boolean tlsAllowInsecureConnection;
    private String tlsTrustCertsFilePath;
    private AuthenticationDataProvider authData;

    public ClientSslContextRefresher(boolean allowInsecure, String trustCertsFilePath,
            AuthenticationDataProvider authData, ScheduledExecutorService eventLoopGroup, long delay, TimeUnit timeUnit)
            throws IOException, GeneralSecurityException {
        super(eventLoopGroup, delay, timeUnit);
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = trustCertsFilePath;
        this.authData = authData;

        if (authData != null && authData.hasDataForTls()) {
            registerFile(authData.getCertFilePath(), authData.getKeyFilePath());
            this.sslContext = SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                    this.tlsTrustCertsFilePath, (X509Certificate[]) authData.getTlsCertificates(),
                    authData.getTlsPrivateKey());
            if (delay > 0) {
                run();
            }
        } else {
            this.sslContext = SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                    this.tlsTrustCertsFilePath);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(ClientSslContextRefresher.class);

    @Override
    public void buildSSLContext() {
        try {
            buildSSLContextWithException();
        } catch (GeneralSecurityException | IOException e) {
            LOG.error("Error occured while trying to create sslContext - using previous one.");
            return;
        }
    }
    
    public void buildSSLContextWithException() throws KeyManagementException, SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        this.sslContext = SecurityUtility.createNettySslContextForClient(this.tlsAllowInsecureConnection,
                this.tlsTrustCertsFilePath, 
                (X509Certificate[]) SecurityUtility.loadCertificatesFromPemFile(authData.getCertFilePath()),
                SecurityUtility.loadPrivateKeyFromPemFile(authData.getKeyFilePath()));
    }

    @Override
    public SslContext get() {
        return sslContext;
    }
}
