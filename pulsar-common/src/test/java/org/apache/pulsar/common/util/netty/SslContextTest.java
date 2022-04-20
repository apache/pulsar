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

package org.apache.pulsar.common.util.netty;

import static org.testng.Assert.assertThrows;
import com.google.common.io.Resources;
import io.netty.handler.ssl.SslProvider;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLException;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.common.util.NettyClientSslContextRefresher;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SslContextTest {
    @DataProvider(name = "caCertSslContextDataProvider")
    public static Object[][] getSslContextDataProvider() {
        Set<String> ciphers = new HashSet<>();
        ciphers.add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
        ciphers.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        ciphers.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        ciphers.add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        ciphers.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        // Note: OPENSSL doesn't support these ciphers.
        return new Object[][]{
                new Object[]{SslProvider.JDK, ciphers},
                new Object[]{SslProvider.JDK, null},

                new Object[]{SslProvider.OPENSSL, ciphers},
                new Object[]{SslProvider.OPENSSL, null},

                new Object[]{null, ciphers},
                new Object[]{null, null},
        };
    }

    @DataProvider(name = "cipherDataProvider")
    public static Object[] getCipher() {
        Set<String> cipher = new HashSet<>();
        cipher.add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
        cipher.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        cipher.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        cipher.add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        cipher.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        return new Object[]{null, cipher};
    }

    @Test(dataProvider = "cipherDataProvider")
    public void testServerKeyStoreSSLContext(Set<String> cipher) throws Exception {
        NettySSLContextAutoRefreshBuilder contextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(null,
                "JKS", Resources.getResource("ssl/jetty_server_key.jks").getPath(),
                "jetty_server_pwd", false, "JKS",
                Resources.getResource("ssl/jetty_server_trust.jks").getPath(),
                "jetty_server_pwd", true, cipher,
                null, 600);
        contextAutoRefreshBuilder.update();
    }

    private static class ClientAuthenticationData implements AuthenticationDataProvider {
        @Override
        public KeyStoreParams getTlsKeyStoreParams() {
            return null;
        }
    }

    @Test(dataProvider = "cipherDataProvider")
    public void testClientKeyStoreSSLContext(Set<String> cipher) throws Exception {
        NettySSLContextAutoRefreshBuilder contextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(null,
                false, "JKS", Resources.getResource("ssl/jetty_server_trust.jks").getPath(),
                "jetty_server_pwd", cipher, null, 0, new ClientAuthenticationData());
        contextAutoRefreshBuilder.update();
    }

    @Test(dataProvider = "caCertSslContextDataProvider")
    public void testServerCaCertSslContextWithSslProvider(SslProvider sslProvider, Set<String> ciphers)
            throws GeneralSecurityException, IOException {
        NettyServerSslContextBuilder sslContext = new NettyServerSslContextBuilder(sslProvider,
                true, Resources.getResource("ssl/my-ca/ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/server-ca.pem").getPath(),
                Resources.getResource("ssl/my-ca/server-key.pem").getPath(),
                ciphers,
                null,
                true, 60);
        if (ciphers != null) {
            if (sslProvider == null || sslProvider == SslProvider.OPENSSL) {
                assertThrows(SSLException.class, sslContext::update);
                return;
            }
        }
        sslContext.update();
    }

    @Test(dataProvider = "caCertSslContextDataProvider")
    public void testClientCaCertSslContextWithSslProvider(SslProvider sslProvider, Set<String> ciphers)
            throws GeneralSecurityException, IOException {
        NettyClientSslContextRefresher sslContext = new NettyClientSslContextRefresher(sslProvider,
                true, Resources.getResource("ssl/my-ca/ca.pem").getPath(),
                null, ciphers, null, 0);
        if (ciphers != null) {
            if (sslProvider == null || sslProvider == SslProvider.OPENSSL) {
                assertThrows(SSLException.class, sslContext::update);
                return;
            }
        }
        sslContext.update();
    }
}
