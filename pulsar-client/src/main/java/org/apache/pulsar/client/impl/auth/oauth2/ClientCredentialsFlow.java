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
package org.apache.pulsar.client.impl.auth.oauth2;

import io.netty.handler.ssl.SslContextBuilder;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.net.ssl.SSLException;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.ClientCredentialsExchangeRequest;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.ClientCredentialsExchanger;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenClient;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenExchangeException;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenResult;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

/**
 * Implementation of OAuth 2.0 Client Credentials flow.
 *
 * @see <a href="https://tools.ietf.org/html/rfc6749#section-4.4">OAuth 2.0 RFC 6749, section 4.4</a>
 */
@Slf4j
class ClientCredentialsFlow extends FlowBase {
    public static final String CONFIG_PARAM_ISSUER_URL = "issuerUrl";
    public static final String CONFIG_PARAM_AUDIENCE = "audience";
    public static final String CONFIG_PARAM_KEY_FILE = "privateKey";
    public static final String CONFIG_PARAM_SCOPE = "scope";
    public static final String CONFIG_PARAM_CONNECT_TIMEOUT = "connectTimeout";
    public static final String CONFIG_PARAM_READ_TIMEOUT = "readTimeout";
    public static final String CONFIG_PARAM_TRUST_CERTS_FILE_PATH = "trustCertsFilePath";

    private static final int DEFAULT_CONNECT_TIMEOUT_IN_SECONDS = 10;
    private static final int DEFAULT_READ_TIMEOUT_IN_SECONDS = 30;

    private static final long serialVersionUID = 1L;

    private final String audience;
    private final String privateKey;
    private final String scope;

    private transient ClientCredentialsExchanger exchanger;

    private boolean initialized = false;

    @Builder
    public ClientCredentialsFlow(URL issuerUrl, String audience, String privateKey, String scope,
                                 Integer connectTimeout, Integer readTimeout, String trustCertsFilePath) {
        super(issuerUrl, getHttpClient(connectTimeout, readTimeout, trustCertsFilePath));
        this.audience = audience;
        this.privateKey = privateKey;
        this.scope = scope;
    }

    private static AsyncHttpClient getHttpClient(Integer connectTimeout, Integer readTimeout,
                                                 String trustCertsFilePath) {
        DefaultAsyncHttpClientConfig.Builder confBuilder = new DefaultAsyncHttpClientConfig.Builder();
        confBuilder.setCookieStore(null);
        confBuilder.setUseProxyProperties(true);
        confBuilder.setFollowRedirect(true);
        confBuilder.setConnectTimeout(
                getConfigValueAsInt(CONFIG_PARAM_CONNECT_TIMEOUT, connectTimeout,
                        DEFAULT_CONNECT_TIMEOUT_IN_SECONDS * 1000));
        confBuilder.setReadTimeout(
                getConfigValueAsInt(CONFIG_PARAM_READ_TIMEOUT, readTimeout, DEFAULT_READ_TIMEOUT_IN_SECONDS * 1000));
        confBuilder.setUserAgent(String.format("Pulsar-Java-v%s", PulsarVersion.getVersion()));
        if (StringUtils.isNotBlank(trustCertsFilePath)) {
            try {
                confBuilder.setSslContext(SslContextBuilder.forClient()
                        .trustManager(new File(trustCertsFilePath))
                        .build());
            } catch (SSLException e) {
                log.error("Could not set trustCertsFilePath", e);
            }
        }
        return new DefaultAsyncHttpClient(confBuilder.build());
    }

    /**
     * Constructs a {@link ClientCredentialsFlow} from configuration parameters.
     *
     * @param params
     * @return
     */
    public static ClientCredentialsFlow fromParameters(Map<String, String> params) {
        URL issuerUrl = parseParameterUrl(params, CONFIG_PARAM_ISSUER_URL);
        String privateKeyUrl = parseParameterString(params, CONFIG_PARAM_KEY_FILE);
        // These are optional parameters, so we only perform a get
        String scope = params.get(CONFIG_PARAM_SCOPE);
        String audience = params.get(CONFIG_PARAM_AUDIENCE);
        Integer connectTimeout = getConfigValueAsInt(params, CONFIG_PARAM_CONNECT_TIMEOUT);
        Integer readTimeout = getConfigValueAsInt(params, CONFIG_PARAM_READ_TIMEOUT);
        String trustCertsFilePath = params.get(CONFIG_PARAM_TRUST_CERTS_FILE_PATH);

        return ClientCredentialsFlow.builder()
                .issuerUrl(issuerUrl)
                .audience(audience)
                .privateKey(privateKeyUrl)
                .scope(scope)
                .connectTimeout(connectTimeout)
                .readTimeout(readTimeout)
                .trustCertsFilePath(trustCertsFilePath)
                .build();
    }

    /**
     * Utility method to get an integer from parameters.
     *
     * @param params the parameters
     * @param key    the key
     * @return the integer value if exists, else null.
     */
    private static Integer getConfigValueAsInt(Map<String, String> params, String key) {
        String value = params.get(key);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException numberFormatException) {
                log.error("Expected configuration for [{}] to be an integer, but got [{}]",
                        key, value, numberFormatException);
            }
        }
        return null;
    }

    /**
     * Utility method to get an integer, or the default value.
     *
     * @param key          the key
     * @param value        the nullable value
     * @param defaultValue the default value
     * @return the value if exits, else the default value.
     */
    private static int getConfigValueAsInt(String key, Integer value, int defaultValue) {
        if (value == null) {
            log.info("Configuration for [{}] is using the default value: [{}]", key, defaultValue);
            return defaultValue;
        } else {
            log.info("Configuration for [{}] is [{}]", key, value);
            return value;
        }
    }

    /**
     * Loads the private key from the given URL.
     *
     * @param privateKeyURL
     * @return
     * @throws IOException
     */
    private static KeyFile loadPrivateKey(String privateKeyURL) throws IOException {
        try {
            URLConnection urlConnection = new org.apache.pulsar.client.api.url.URL(privateKeyURL).openConnection();
            try {
                String protocol = urlConnection.getURL().getProtocol();
                String contentType = urlConnection.getContentType();
                if ("data".equals(protocol) && !"application/json".equals(contentType)) {
                    throw new IllegalArgumentException(
                            "Unsupported media type or encoding format: " + urlConnection.getContentType());
                }
                KeyFile privateKey;
                try (Reader r = new InputStreamReader((InputStream) urlConnection.getContent(),
                        StandardCharsets.UTF_8)) {
                    privateKey = KeyFile.fromJson(r);
                }
                return privateKey;
            } finally {
                IOUtils.close(urlConnection);
            }
        } catch (URISyntaxException | InstantiationException | IllegalAccessException e) {
            throw new IOException("Invalid privateKey format", e);
        }
    }

    @Override
    public void initialize() throws PulsarClientException {
        super.initialize();
        assert this.metadata != null;

        URL tokenUrl = this.metadata.getTokenEndpoint();
        this.exchanger = new TokenClient(tokenUrl, httpClient);
        initialized = true;
    }

    public TokenResult authenticate() throws PulsarClientException {
        // read the private key from storage
        KeyFile keyFile;
        try {
            keyFile = loadPrivateKey(this.privateKey);
        } catch (IOException e) {
            throw new PulsarClientException.AuthenticationException("Unable to read private key: " + e.getMessage());
        }

        // request an access token using client credentials
        ClientCredentialsExchangeRequest req = ClientCredentialsExchangeRequest.builder()
                .clientId(keyFile.getClientId())
                .clientSecret(keyFile.getClientSecret())
                .audience(this.audience)
                .scope(this.scope)
                .build();
        TokenResult tr;
        if (!initialized) {
            initialize();
        }
        try {
            tr = this.exchanger.exchangeClientCredentials(req);
        } catch (TokenExchangeException | IOException e) {
            throw new PulsarClientException.AuthenticationException("Unable to obtain an access token: "
                    + e.getMessage());
        }

        return tr;
    }

    @Override
    public void close() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
        if (exchanger != null) {
            exchanger.close();
        }
    }
}
