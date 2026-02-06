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

import io.swagger.annotations.ApiModelProperty;
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

    @ApiModelProperty(
            name = "tlsCiphers",
            value = "TLS ciphers to be used",
            required = true
    )
    private Set<String> tlsCiphers;

    @ApiModelProperty(
            name = "tlsProtocols",
            value = "TLS protocols to be used",
            required = true
    )
    private Set<String> tlsProtocols;

    @ApiModelProperty(
            name = "allowInsecureConnection",
            value = "Insecure Connections are allowed",
            required = true
    )
    private boolean allowInsecureConnection;

    @ApiModelProperty(
            name = "requireTrustedClientCertOnConnect",
            value = "Require trusted client certificate on connect",
            required = true
    )
    private boolean requireTrustedClientCertOnConnect;

    @ApiModelProperty(
            name = "authData",
            value = "Authentication Data Provider utilized by the Client for identification"
    )
    private AuthenticationDataProvider authData;

    @ApiModelProperty(
            name = "tlsCustomParams",
            value = "Custom Parameters required by Pulsar SSL factory plugins"
    )
    private String tlsCustomParams;

    @ApiModelProperty(
            name = "tlsProvider",
            value = "TLS Provider to be used"
    )
    private String tlsProvider;

    @ApiModelProperty(
            name = "tlsTrustStoreType",
            value = "TLS Trust Store Type to be used"
    )
    private String tlsTrustStoreType;

    @ApiModelProperty(
            name = "tlsTrustStorePath",
            value = "TLS Trust Store Path"
    )
    private String tlsTrustStorePath;

    @ApiModelProperty(
            name = "tlsTrustStorePassword",
            value = "TLS Trust Store Password"
    )
    private String tlsTrustStorePassword;

    @ApiModelProperty(
            name = "tlsTrustCertsFilePath",
            value = " TLS Trust certificates file path"
    )
    private String tlsTrustCertsFilePath;

    @ApiModelProperty(
            name = "tlsCertificateFilePath",
            value = "Path for the TLS Certificate file"
    )
    private String tlsCertificateFilePath;

    @ApiModelProperty(
            name = "tlsKeyFilePath",
            value = "Path for TLS Private key file"
    )
    private String tlsKeyFilePath;

    @ApiModelProperty(
            name = "tlsKeyStoreType",
            value = "TLS Key Store Type to be used"
    )
    private String tlsKeyStoreType;

    @ApiModelProperty(
            name = "tlsKeyStorePath",
            value = "TLS Key Store Path"
    )
    private String tlsKeyStorePath;

    @ApiModelProperty(
            name = "tlsKeyStorePassword",
            value = "TLS Key Store Password"
    )
    private String tlsKeyStorePassword;

    @ApiModelProperty(
            name = "isTlsEnabledWithKeystore",
            value = "TLS configuration enabled with key store configs"
    )
    private boolean tlsEnabledWithKeystore;

    @ApiModelProperty(
            name = "isServerMode",
            value = "Is the SSL Configuration for a Server or Client",
            required = true
    )
    private boolean serverMode;

    @ApiModelProperty(
            name = "isHttps",
            value = "Is the SSL Configuration for a Http client or Server"
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