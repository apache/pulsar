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
package org.apache.pulsar.common.tls;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.SecurityUtility;

@Slf4j
public class KeyStoreSslContextSupplier implements Supplier<SslContext> {
    private volatile InputStream keyStoreStream;
    private volatile InputStream trustStoreStream;

    private final KeyStoreSslContextConf conf;

    private volatile SslContext sslContext;

    private final long refreshIntervalMillis;
    private long lastRefreshTimestamp;
    private final Clock clock = Clock.systemUTC();

    public SslContext newSslContext()
            throws IOException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException,
            CertificateException {

        Provider sslContextProvider = SecurityUtility.getSslContextProvider(conf.getSslContextProvider());
        SslProvider sslProvider = SecurityUtility.getSslProvider(conf.getSslProvider());

        KeyManagerFactory keyManagerFactory;
        if (conf.getKeyStoreStreamSupplier() != null) {
            keyStoreStream = conf.getKeyStoreStreamSupplier().get();
            keyManagerFactory = SecurityUtility.newKeyManagerFactory(conf.getKeyStoreType(),
                    keyStoreStream,
                    StringUtils.isEmpty(conf.getKeyStorePassword()) ? new char[]{} :
                            conf.getKeyStorePassword().toCharArray());
        } else {
            keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        }

        TrustManager[] trustManagers;
        if (conf.getTrustStoreStreamSupplier() != null) {
            trustStoreStream = conf.getTrustStoreStreamSupplier().get();
            trustManagers = SecurityUtility.setupTrustCerts(conf.getTrustStoreType(), conf.isAllowInsecureConnection(),
                    trustStoreStream,
                    StringUtils.isEmpty(conf.getTrustStorePassword()) ? new char[]{} :
                            conf.getTrustStorePassword().toCharArray(), sslContextProvider);
        } else {
            throw new IllegalArgumentException("Trust store cannot be null");
        }

        if (trustManagers == null || trustManagers.length != 1) {
            throw new KeyStoreException("TrustManager size should be 1");
        }

        SslContextBuilder sslContextBuilder;
        if (conf.isForClient()) {
            sslContextBuilder = SslContextBuilder.forClient().keyManager(keyManagerFactory);
        } else {
            sslContextBuilder = SslContextBuilder.forServer(keyManagerFactory)
                    .clientAuth(conf.isRequireTrustedClientCertOnConnect() ? ClientAuth.REQUIRE : ClientAuth.OPTIONAL);
        }
        sslContextBuilder.sslProvider(sslProvider)
                .sslContextProvider(sslContextProvider)
                .ciphers(conf.getCiphers())
                .protocols(conf.getProtocols());
        sslContextBuilder.trustManager(trustManagers[0]);
        return sslContextBuilder.build();
    }

    public KeyStoreSslContextSupplier(KeyStoreSslContextConf conf) {
        this.conf = conf;
        this.refreshIntervalMillis = TimeUnit.SECONDS.toMillis(conf.getRefreshIntervalSeconds());
    }

    protected boolean needUpdate() {
        if (refreshIntervalMillis == 0) {
            return true;
        }

        if (sslContext == null) {
            return true;
        }

        if (conf.getRefreshIntervalSeconds() < 0) {
            return false;
        }

        long now = clock.millis();
        if ((now - lastRefreshTimestamp) < refreshIntervalMillis) {
            return false;
        }

        if (keyStoreStream != null && !keyStoreStream.equals(conf.getKeyStoreStreamSupplier().get())) {
            return true;
        }

        return trustStoreStream != null && !trustStoreStream.equals(conf.getTrustStoreStreamSupplier().get());
    }

    @Override
    public synchronized SslContext get() {
        if (needUpdate()) {
            try {
                sslContext = newSslContext();
                lastRefreshTimestamp = clock.millis();
            } catch (Exception e) {
                log.error("Exception while trying to refresh ssl Context {} ", e.getMessage(), e);
            }
        }
        return sslContext;
    }
}
