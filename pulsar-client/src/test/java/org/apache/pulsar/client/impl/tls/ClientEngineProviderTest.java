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
package org.apache.pulsar.client.impl.tls;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslProvider;
import org.testng.annotations.Test;

/**
 * The client's v4 {@code sslProvider} -> Netty engine mapping must match the server-side
 * {@code TlsFactorySupport.engineProvider} (PIP-478). Two properties are easy to lose and both are
 * behaviour-visible: OPENSSL_REFCNT must not collapse to OPENSSL (only the reference-counted variant avoids
 * the finalize() JEP 421 deprecates), and an unset provider must keep selecting the native engine when
 * tcnative is present, which is what the PIP-337 client did by passing null to SslContextBuilder.
 */
public class ClientEngineProviderTest {

    @Test
    public void engineLiteralsAreHonouredVerbatim() {
        assertThat(ClientTlsFactorySupport.engineProvider("OPENSSL")).isEqualTo(SslProvider.OPENSSL);
        assertThat(ClientTlsFactorySupport.engineProvider("OPENSSL_REFCNT"))
                .as("OPENSSL_REFCNT must not collapse to OPENSSL")
                .isEqualTo(SslProvider.OPENSSL_REFCNT);
        assertThat(ClientTlsFactorySupport.engineProvider("openssl_refcnt"))
                .isEqualTo(SslProvider.OPENSSL_REFCNT);
        assertThat(ClientTlsFactorySupport.engineProvider("JDK")).isEqualTo(SslProvider.JDK);
    }

    @Test
    public void aJsseProviderNameSelectsNoNativeEngine() {
        // Conscrypt / BCJSSE belong on the jsseProvider axis; they must not be read as an engine.
        assertThat(ClientTlsFactorySupport.engineProvider("Conscrypt")).isEqualTo(SslProvider.JDK);
        assertThat(ClientTlsFactorySupport.engineProvider("BCJSSE")).isEqualTo(SslProvider.JDK);
    }

    @Test
    public void unsetSelectsTheNativeEngineWhenAvailable() {
        SslProvider expected = OpenSsl.isAvailable() ? SslProvider.OPENSSL_REFCNT : SslProvider.JDK;
        assertThat(ClientTlsFactorySupport.engineProvider(null)).isEqualTo(expected);
        assertThat(ClientTlsFactorySupport.engineProvider("")).isEqualTo(expected);
        assertThat(ClientTlsFactorySupport.engineProvider("   ")).isEqualTo(expected);
    }
}
