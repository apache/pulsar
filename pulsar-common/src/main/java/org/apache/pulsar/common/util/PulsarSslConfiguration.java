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
package org.apache.pulsar.common.util;

import io.swagger.v3.oas.annotations.media.Schema;
import java.io.Serializable;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.pulsar.client.api.AuthenticationDataProvider;

/**
 * Pulsar SSL Configuration Object to be used by all Pulsar Server and Client Components.
 */
@Builder
@Getter
@ToString
public class PulsarSslConfiguration implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    @Schema(
            name = "tlsCiphers",
            description = "TLS ciphers to be used",
            requiredMode = Schema.RequiredMode.REQUIRED
    )
    private Set<String> tlsCiphers;

    @Schema(
            name = "tlsProtocols",
            description = "TLS protocols to be used",
            requiredMode = Schema.RequiredMode.REQUIRED
    )
    private Set<String> tlsProtocols;

    @Schema(
            name = "allowInsecureConnection",
            description = "Insecure Connections are allowed",
            requiredMode = Schema.RequiredMode.REQUIRED
    )
    private boolean allowInsecureConnection;

    @Schema(
            name = "requireTrustedClientCertOnConnect",
            description = "Require trusted client certificate on connect",
            requiredMode = Schema.RequiredMode.REQUIRED
    )
    private boolean requireTrustedClientCertOnConnect;

    @Schema(
            name = "authData",
            description = "Authentication Data Provider utilized by the Client for identification"
    )
    private AuthenticationDataProvider authData;

    @Schema(
            name = "tlsCustomParams",
            description = "Custom Parameters required by Pulsar SSL factory plugins"
    )
    private String tlsCustomParams;

    @Schema(
            name = "tlsProvider",
            description = "TLS Provider to be used"
    )
    private String tlsProvider;

    @Schema(
            name = "tlsTrustStoreType",
            description = "TLS Trust Store Type to be used"
    )
    private String tlsTrustStoreType;

    @Schema(
            name = "tlsTrustStorePath",
            description = "TLS Trust Store Path"
    )
    private String tlsTrustStorePath;

    @Schema(
            name = "tlsTrustStorePassword",
            description = "TLS Trust Store Password"
    )
    private String tlsTrustStorePassword;

    @Schema(
            name = "tlsTrustCertsFilePath",
            description = "TLS Trust certificates file path"
    )
    private String tlsTrustCertsFilePath;

    @Schema(
            name = "tlsCertificateFilePath",
            description = "Path for the TLS Certificate file"
    )
    private String tlsCertificateFilePath;

    @Schema(
            name = "tlsKeyFilePath",
            description = "Path for TLS Private key file"
    )
    private String tlsKeyFilePath;

    @Schema(
            name = "tlsKeyStoreType",
            description = "TLS Key Store Type to be used"
    )
    private String tlsKeyStoreType;

    @Schema(
            name = "tlsKeyStorePath",
            description = "TLS Key Store Path"
    )
    private String tlsKeyStorePath;

    @Schema(
            name = "tlsKeyStorePassword",
            description = "TLS Key Store Password"
    )
    private String tlsKeyStorePassword;

    @Schema(
            name = "isTlsEnabledWithKeystore",
            description = "TLS configuration enabled with key store configs"
    )
    private boolean tlsEnabledWithKeystore;

    @Schema(
            name = "isServerMode",
            description = "Is the SSL Configuration for a Server or Client",
            requiredMode = Schema.RequiredMode.REQUIRED
    )
    private boolean serverMode;

    @Schema(
            name = "isHttps",
            description = "Is the SSL Configuration for a Http client or Server"
    )
    private boolean isHttps;

    @Override
    public PulsarSslConfiguration clone() {
        try {
            return (PulsarSslConfiguration) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone PulsarSslConfiguration", e);
        }
    }

}