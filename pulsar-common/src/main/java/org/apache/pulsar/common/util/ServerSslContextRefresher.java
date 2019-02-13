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
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslContext;

public class ServerSslContextRefresher extends SslContextRefresher {
    private final boolean tlsAllowInsecureConnection;
    private final String tlsTrustCertsFilePath;
    private final String tlsCertificateFilePath;
    private final String tlsKeyFilePath;
    private final Set<String> tlsCiphers;
    private final Set<String> tlsProtocols;
    private final boolean tlsRequireTrustedClientCertOnConnect;
    private volatile SslContext sslContext;

    public ServerSslContextRefresher(boolean allowInsecure, String trustCertsFilePath, String certificateFilePath,
            String keyFilePath, Set<String> ciphers, Set<String> protocols, boolean requireTrustedClientCertOnConnect,
            ScheduledExecutorService eventLoopGroup, long delay, TimeUnit timeUnit)
            throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        super(eventLoopGroup, delay, timeUnit);
        this.tlsAllowInsecureConnection = allowInsecure;
        this.tlsTrustCertsFilePath = trustCertsFilePath;
        this.tlsCertificateFilePath = certificateFilePath;
        this.tlsKeyFilePath = keyFilePath;
        this.tlsCiphers = ciphers;
        this.tlsProtocols = protocols;
        this.tlsRequireTrustedClientCertOnConnect = requireTrustedClientCertOnConnect;

        registerFile(tlsTrustCertsFilePath, tlsCertificateFilePath, tlsKeyFilePath);
        
        buildSSLContextWithException();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Certs will be refreshed every {} minutes", delay);
        }

        if (delay > 0) {
            run();
        }
    }

    @Override
    public void buildSSLContext() {
        try {
            buildSSLContextWithException();
        } catch (GeneralSecurityException | IOException e) {
            LOG.error("Error occured while trying to create sslContext - using previous one.");
            return;
        }
    }
    
    public void buildSSLContextWithException() throws SSLException, FileNotFoundException, GeneralSecurityException, IOException {
        this.sslContext = SecurityUtility.createNettySslContextForServer(tlsAllowInsecureConnection,
                tlsTrustCertsFilePath, tlsCertificateFilePath, tlsKeyFilePath, tlsCiphers, tlsProtocols,
                tlsRequireTrustedClientCertOnConnect);
    }

    @Override
    public SslContext get() {
        return this.sslContext;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ServerSslContextRefresher.class);
}
