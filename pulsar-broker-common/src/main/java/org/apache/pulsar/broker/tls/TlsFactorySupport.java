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

import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.common.tls.TlsClientAuthMode;

/**
 * Resolves the {@code tlsClientAuthMode} setting of the server components (broker, proxy, websocket proxy and
 * functions worker) that have TLS listeners.
 */
@Slf4j
public final class TlsFactorySupport {

    private TlsFactorySupport() {
    }

    /**
     * Resolve a server component's effective {@link TlsClientAuthMode} from its configuration; see
     * {@link TlsClientAuthMode#resolve}. TLS client-certificate authentication counts as enabled when
     * authentication is enabled and an {@link AuthenticationProviderTls} (or a subclass) is among the
     * authentication providers. A custom provider that reads client certificates otherwise needs an explicit
     * {@code tlsClientAuthMode}.
     *
     * @param configured                        the configured {@code tlsClientAuthMode}, or {@code null}
     * @param requireTrustedClientCertOnConnect the deprecated {@code tlsRequireTrustedClientCertOnConnect} setting
     * @param authenticationEnabled             the {@code authenticationEnabled} setting
     * @param authenticationProviders           the {@code authenticationProviders} class names
     * @return the effective mode
     * @throws IllegalArgumentException if the settings conflict
     */
    public static TlsClientAuthMode resolveClientAuthMode(TlsClientAuthMode configured,
                                                          boolean requireTrustedClientCertOnConnect,
                                                          boolean authenticationEnabled,
                                                          Set<String> authenticationProviders) {
        return TlsClientAuthMode.resolve(configured, requireTrustedClientCertOnConnect,
                isClientCertAuthenticationEnabled(authenticationEnabled, authenticationProviders));
    }

    /**
     * Resolve the effective {@link TlsClientAuthMode} like {@link #resolveClientAuthMode} and log it together with
     * where it came from. A component with a TLS listener calls this once when it starts, so the log explains why
     * a client certificate is or is not requested.
     *
     * @param component                         the component name for the log, such as {@code broker}
     * @param configured                        the configured {@code tlsClientAuthMode}, or {@code null}
     * @param requireTrustedClientCertOnConnect the deprecated {@code tlsRequireTrustedClientCertOnConnect} setting
     * @param authenticationEnabled             the {@code authenticationEnabled} setting
     * @param authenticationProviders           the {@code authenticationProviders} class names
     * @return the effective mode
     * @throws IllegalArgumentException if the settings conflict
     */
    public static TlsClientAuthMode logClientAuthMode(String component, TlsClientAuthMode configured,
                                                      boolean requireTrustedClientCertOnConnect,
                                                      boolean authenticationEnabled,
                                                      Set<String> authenticationProviders) {
        boolean clientCertAuthenticationEnabled =
                isClientCertAuthenticationEnabled(authenticationEnabled, authenticationProviders);
        TlsClientAuthMode mode = TlsClientAuthMode.resolve(configured, requireTrustedClientCertOnConnect,
                clientCertAuthenticationEnabled);
        String source;
        if (configured != null) {
            source = "configured";
        } else if (requireTrustedClientCertOnConnect) {
            source = "derived: deprecated tlsRequireTrustedClientCertOnConnect=true";
        } else if (clientCertAuthenticationEnabled) {
            source = "derived: AuthenticationProviderTls enabled";
        } else if (!authenticationEnabled) {
            source = "derived: authentication disabled";
        } else {
            source = "derived: AuthenticationProviderTls not among authenticationProviders";
        }
        log.info("TLS client auth mode: component={}, mode={}, source={}", component, mode, source);
        return mode;
    }

    private static boolean isClientCertAuthenticationEnabled(boolean authenticationEnabled,
                                                             Set<String> authenticationProviders) {
        return authenticationEnabled && authenticationProviders != null
                && authenticationProviders.stream().anyMatch(TlsFactorySupport::isTlsAuthenticationProvider);
    }

    private static boolean isTlsAuthenticationProvider(String className) {
        if (StringUtils.isBlank(className)) {
            return false;
        }
        String name = className.trim();
        if (AuthenticationProviderTls.class.getName().equals(name)) {
            return true;
        }
        try {
            // Loaded like AuthenticationService loads the providers, to recognize a subclass.
            return AuthenticationProviderTls.class.isAssignableFrom(Class.forName(name));
        } catch (ClassNotFoundException | LinkageError e) {
            // AuthenticationService reports the unloadable provider when it starts.
            return false;
        }
    }
}
