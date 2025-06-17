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

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.ClientCredentialsExchanger;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.JwtBearerExchangeRequest;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenClient;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenExchangeException;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.TokenResult;

@Slf4j
public class JwtBearerFlow extends FlowBase {

    public static final String SIGNATURE_ALGORITHM = "RS256";

    public static final String CONFIG_PARAM_SIGNATURE_ALGORITHM = "signatureAlgorithm";

    public static final String CONFIG_PARAM_ISSUER_URL = "issuerUrl";
    public static final String CONFIG_PARAM_AUDIENCE = "audience";
    public static final String CONFIG_PARAM_KEY_FILE = "privateKey";
    public static final String CONFIG_PARAM_TOKEN_TTL = "tokenTTlMillis";

    private static final long serialVersionUID = 1L;

    private final String audience;
    private final String privateKey;
    private final String signatureAlgorithm;
    private final long ttlMillis;

    private transient ClientCredentialsExchanger exchanger;

    private boolean initialized = false;

    public static JwtBearerFlow fromParameters(Map<String, String> params) {
        URL issuerUrl = parseParameterUrl(params, CONFIG_PARAM_ISSUER_URL);
        String privateKeyUrl = parseParameterString(params, CONFIG_PARAM_KEY_FILE);
        // These are optional parameters, so we only perform a get
        String audience = params.get(CONFIG_PARAM_AUDIENCE);
        String signatureAlgorithm = params.getOrDefault(CONFIG_PARAM_SIGNATURE_ALGORITHM, SIGNATURE_ALGORITHM);
        long ttlMillis = Long.parseLong(params.getOrDefault(CONFIG_PARAM_TOKEN_TTL, "3600000"));

        return JwtBearerFlow.builder()
                .issuerUrl(issuerUrl)
                .audience(audience)
                .privateKey(privateKeyUrl)
                .signatureAlgorithm(signatureAlgorithm)
                .ttlMillis(ttlMillis)
                .build();
    }

    @Builder
    public JwtBearerFlow(URL issuerUrl,
                         String audience,
                         String privateKey,
                         String signatureAlgorithm,
                         long ttlMillis) {
        super(issuerUrl);
        this.audience = audience;
        this.privateKey = privateKey;
        this.signatureAlgorithm = signatureAlgorithm;
        this.ttlMillis = ttlMillis;
    }

    @Override
    public void initialize() throws PulsarClientException {
        super.initialize();
        assert this.metadata != null;

        URL tokenUrl = this.metadata.getTokenEndpoint();
        this.exchanger = new TokenClient(tokenUrl);
        initialized = true;
    }

    @Override
    public TokenResult authenticate() throws PulsarClientException {
        // read the private key from storage
        KeyFile keyFile;
        try {
            keyFile = KeyFile.loadPrivateKey(this.privateKey);
        } catch (IOException e) {
            throw new PulsarClientException.AuthenticationException("Unable to read private key: " + e.getMessage());
        }

        // request an access token using client credentials
        final String jwtAssertion;
        try {
            jwtAssertion = JwtBearerExchangeRequest.generateJWT(
                    keyFile.getClientId(),
                    this.audience,
                    keyFile.getClientSecret(),
                    this.signatureAlgorithm,
                    ttlMillis);
        } catch (Exception e) {
            throw new PulsarClientException("Failed to generate JWT", e);
        }
        TokenResult tr;
        if (!initialized) {
            initialize();
        }

        try {
            JwtBearerExchangeRequest req = JwtBearerExchangeRequest.builder()
                    .assertion(jwtAssertion)
                    .build();
            tr = this.exchanger.exchangeClientCredentials(req);
        } catch (TokenExchangeException | IOException e) {
            throw new PulsarClientException.AuthenticationException("Unable to obtain an access token: "
                    + e.getMessage());
        }

        return tr;
    }

    @Override
    public void close() throws Exception {
        if (exchanger != null) {
            exchanger.close();
        }
    }
}
