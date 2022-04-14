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
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.KeyStoreHolder;
import org.apache.pulsar.common.util.SecurityUtility;

@Slf4j
public class SslContextSupplier implements Supplier<SslContext> {
    private volatile X509Certificate[] trustCert;
    private volatile PrivateKey key;
    private volatile X509Certificate[] cert;

    private final SslContextConf conf;

    private volatile SslContext sslContext;

    private final long refreshIntervalMillis;
    private long lastRefreshTimestamp;
    private final Clock clock = Clock.systemUTC();

    public SslContext newSslContext()
            throws SSLException, KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
        trustCert = conf.getTrustCertSupplier().get();
        key = conf.getKeySupplier().get();
        cert = conf.getCertSupplier().get();

        Provider sslContextProvider = SecurityUtility.getSslContextProvider(conf.getSslContextProvider());
        SslProvider sslProvider = SecurityUtility.getSslProvider(conf.getSslProvider());

        KeyManagerFactory keyManagerFactory =
                SecurityUtility.newKeyManagerFactory(key, cert);

        TrustManager[] trustManagers =
                SecurityUtility.setupTrustCerts(new KeyStoreHolder(), conf.isAllowInsecureConnection(),
                        trustCert, sslContextProvider);
        if (trustManagers != null && trustManagers.length != 1) {
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
        if (trustManagers != null) {
            sslContextBuilder.trustManager(trustManagers[0]);
        }
        return sslContextBuilder.build();
    }

    public SslContextSupplier(SslContextConf conf) {
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

        if (!Arrays.equals(conf.getTrustCertSupplier().get(), trustCert)) {
            return true;
        }

        if (!Objects.equals(conf.getKeySupplier().get(), key)) {
            return true;
        }

        if (!Arrays.equals(conf.getCertSupplier().get(), cert)) {
            return true;
        }

        return false;
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
