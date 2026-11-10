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
package org.apache.pulsar.broker.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.Set;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.common.tls.TlsClientAuthMode;
import org.testng.annotations.Test;

public class TlsFactorySupportTest {
    @Test
    public void resolveClientAuthModeDetectsTlsAuthenticationProvider() {
        Set<String> tls = Set.of(AuthenticationProviderToken.class.getName(),
                AuthenticationProviderTls.class.getName());
        Set<String> tlsSubclass = Set.of(TlsSubclassProvider.class.getName());
        Set<String> tokenOnly = Set.of(AuthenticationProviderToken.class.getName(), "com.example.Missing");
        assertThat(TlsFactorySupport.resolveClientAuthMode(null, false, true, tls))
                .isEqualTo(TlsClientAuthMode.OPTIONAL);
        assertThat(TlsFactorySupport.resolveClientAuthMode(null, false, true, tlsSubclass))
                .isEqualTo(TlsClientAuthMode.OPTIONAL);
        assertThat(TlsFactorySupport.resolveClientAuthMode(null, false, true, tokenOnly))
                .isEqualTo(TlsClientAuthMode.NONE);
        assertThat(TlsFactorySupport.resolveClientAuthMode(null, false, false, tls))
                .as("authentication disabled").isEqualTo(TlsClientAuthMode.NONE);
        assertThat(TlsFactorySupport.resolveClientAuthMode(null, false, true, null))
                .isEqualTo(TlsClientAuthMode.NONE);
        assertThat(TlsFactorySupport.resolveClientAuthMode(null, true, false, Set.of()))
                .isEqualTo(TlsClientAuthMode.REQUIRE);
        assertThat(TlsFactorySupport.resolveClientAuthMode(TlsClientAuthMode.OPTIONAL, false, false, tokenOnly))
                .isEqualTo(TlsClientAuthMode.OPTIONAL);
        assertThat(TlsFactorySupport.resolveClientAuthMode(TlsClientAuthMode.NONE, false, true, tokenOnly))
                .isEqualTo(TlsClientAuthMode.NONE);
        assertThatThrownBy(() -> TlsFactorySupport.resolveClientAuthMode(TlsClientAuthMode.NONE, false, true,
                tlsSubclass)).isInstanceOf(IllegalArgumentException.class);
        assertThat(TlsFactorySupport.resolveClientAuthMode(TlsClientAuthMode.NONE, false, false, tls))
                .as("authentication disabled").isEqualTo(TlsClientAuthMode.NONE);
    }

    @Test
    public void logClientAuthModeResolvesLikeResolveClientAuthMode() {
        Set<String> tls = Set.of(AuthenticationProviderTls.class.getName());
        assertThat(TlsFactorySupport.logClientAuthMode("test", null, false, true, tls))
                .isEqualTo(TlsClientAuthMode.OPTIONAL);
        assertThat(TlsFactorySupport.logClientAuthMode("test", null, false, false, tls))
                .isEqualTo(TlsClientAuthMode.NONE);
        assertThat(TlsFactorySupport.logClientAuthMode("test", null, true, false, Set.of()))
                .isEqualTo(TlsClientAuthMode.REQUIRE);
        assertThatThrownBy(() -> TlsFactorySupport.logClientAuthMode("test", TlsClientAuthMode.NONE, false, true,
                tls)).isInstanceOf(IllegalArgumentException.class);
    }

    public static class TlsSubclassProvider extends AuthenticationProviderTls {
    }
}
