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
package org.apache.pulsar.client.admin;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;

/**
 * Builder class for a {@link PulsarAdmin} instance.
 *
 */
public interface PulsarAdminBuilder {

    /**
     * @return the new {@link PulsarAdmin} instance
     */
    PulsarAdmin build() throws PulsarClientException;

    /**
     * Create a copy of the current client builder.
     * <p/>
     * Cloning the builder can be used to share an incomplete configuration and specialize it multiple times. For
     * example:
     *
     * <pre>
     * PulsarAdminBuilder builder = PulsarAdmin.builder().allowTlsInsecureConnection(false);
     *
     * PulsarAdmin client1 = builder.clone().serviceHttpUrl(URL_1).build();
     * PulsarAdmin client2 = builder.clone().serviceHttpUrl(URL_2).build();
     * </pre>
     */
    PulsarAdminBuilder clone();

    /**
     * Set the Pulsar service HTTP URL for the admin endpoint (eg. "http://my-broker.example.com:8080", or
     * "https://my-broker.example.com:8443" for TLS)
     */
    PulsarAdminBuilder serviceHttpUrl(String serviceHttpUrl);

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p/>
     * Example:
     * <p/>
     *
     * <pre>
     * <code>
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     * String AUTH_PARAMS = "tlsCertFile:/my/cert/file,tlsKeyFile:/my/key/file";
     *
     * PulsarAdmin client = PulsarAdmin.builder()
     *          .serviceHttpUrl(SERVICE_HTTP_URL)
     *          .authentication(AUTH_CLASS, AUTH_PARAMS)
     *          .build();
     * ....
     * </code>
     * </pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    PulsarAdminBuilder authentication(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException;

    /**
     * Set the authentication provider to use in the Pulsar client instance.
     * <p/>
     * Example:
     * <p/>
     *
     * <pre>{@code
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     *
     * Map<String, String> conf = new TreeMap<>();
     * conf.put("tlsCertFile", "/my/cert/file");
     * conf.put("tlsKeyFile", "/my/key/file");
     *
     * PulsarAdmin client = PulsarAdmin.builder()
     *          .serviceHttpUrl(SERVICE_HTTP_URL)
     *          .authentication(AUTH_CLASS, conf)
     *          .build();
     * ....
     * }
     * </pre>
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     *             failed to instantiate specified Authentication-Plugin
     */
    PulsarAdminBuilder authentication(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException;

    /**
     * Set the authentication provider to use in the Pulsar admin instance.
     * <p/>
     * Example:
     * <p/>
     *
     * <pre>{@code
     * String AUTH_CLASS = "org.apache.pulsar.client.impl.auth.AuthenticationTls";
     *
     * Map<String, String> conf = new TreeMap<>();
     * conf.put("tlsCertFile", "/my/cert/file");
     * conf.put("tlsKeyFile", "/my/key/file");
     *
     * Authentication auth = AuthenticationFactor.create(AUTH_CLASS, conf);
     *
     * PulsarAdmin admin = PulsarAdmin.builder()
     *          .serviceHttpUrl(SERVICE_URL)
     *          .authentication(auth)
     *          .build();
     * ....
     * }
     * </pre>
     *
     * @param authentication
     *            an instance of the {@link Authentication} provider already constructed
     */
    PulsarAdminBuilder authentication(Authentication authentication);

    /**
     * Set the path to the trusted TLS certificate file.
     *
     * @param tlsTrustCertsFilePath
     */
    PulsarAdminBuilder tlsTrustCertsFilePath(String tlsTrustCertsFilePath);

    /**
     * Configure whether the Pulsar admin client accept untrusted TLS certificate from broker <i>(default: false)</i>.
     *
     * @param allowTlsInsecureConnection
     */
    PulsarAdminBuilder allowTlsInsecureConnection(boolean allowTlsInsecureConnection);

    /**
     * It allows to validate hostname verification when client connects to broker over TLS. It validates incoming x509
     * certificate and matches provided hostname(CN/SAN) with expected broker's host name. It follows RFC 2818, 3.1.
     * Server Identity hostname verification.
     *
     * @see <a href="https://tools.ietf.org/html/rfc2818">rfc2818</a>
     *
     * @param enableTlsHostnameVerification
     */
    PulsarAdminBuilder enableTlsHostnameVerification(boolean enableTlsHostnameVerification);

    /**
     * If Tls is enabled, whether use KeyStore type as tls configuration parameter.
     * False means use default pem type configuration.
     *
     * @param useKeyStoreTls
     */
    PulsarAdminBuilder useKeyStoreTls(boolean useKeyStoreTls);

    /**
     * The name of the security provider used for SSL connections.
     * Default value is the default security provider of the JVM.
     *
     * @param sslProvider
     */
    PulsarAdminBuilder sslProvider(String sslProvider);

    /**
     * The file format of the trust store file.
     *
     * @param tlsTrustStoreType
     */
    PulsarAdminBuilder tlsTrustStoreType(String tlsTrustStoreType);

    /**
     * The location of the trust store file.
     *
     * @param tlsTrustStorePath
     */
    PulsarAdminBuilder tlsTrustStorePath(String tlsTrustStorePath);

    /**
     * The store password for the key store file.
     *
     * @param tlsTrustStorePassword
     * @return the client builder instance
     */
    PulsarAdminBuilder tlsTrustStorePassword(String tlsTrustStorePassword);

    /**
     * A list of cipher suites.
     * This is a named combination of authentication, encryption, MAC and key exchange algorithm
     * used to negotiate the security settings for a network connection using TLS or SSL network protocol.
     * By default all the available cipher suites are supported.
     *
     * @param tlsCiphers
     */
    PulsarAdminBuilder tlsCiphers(Set<String> tlsCiphers);

    /**
     * The SSL protocol used to generate the SSLContext.
     * Default setting is TLS, which is fine for most cases.
     * Allowed values in recent JVMs are TLS, TLSv1.3, TLSv1.2 and TLSv1.1.
     *
     * @param tlsProtocols
     */
    PulsarAdminBuilder tlsProtocols(Set<String> tlsProtocols);

    /**
     * This sets the connection time out for the pulsar admin client.
     *
     * @param connectionTimeout
     * @param connectionTimeoutUnit
     */
    PulsarAdminBuilder connectionTimeout(int connectionTimeout, TimeUnit connectionTimeoutUnit);

    /**
     * This sets the server response read time out for the pulsar admin client for any request.
     *
     * @param readTimeout
     * @param readTimeoutUnit
     */
    PulsarAdminBuilder readTimeout(int readTimeout, TimeUnit readTimeoutUnit);

    /**
     * This sets the server request time out for the pulsar admin client for any request.
     *
     * @param requestTimeout
     * @param requestTimeoutUnit
     */
    PulsarAdminBuilder requestTimeout(int requestTimeout, TimeUnit requestTimeoutUnit);

    /**
     * This sets auto cert refresh time if Pulsar admin uses tls authentication.
     *
     * @param autoCertRefreshTime
     * @param autoCertRefreshTimeUnit
     */
    PulsarAdminBuilder autoCertRefreshTime(int autoCertRefreshTime, TimeUnit autoCertRefreshTimeUnit);
    /**
     *
     * @return
     */
    PulsarAdminBuilder setContextClassLoader(ClassLoader clientBuilderClassLoader);

}
