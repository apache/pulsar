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

package org.apache.pulsar.client.impl.auth;

import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.sasl.SaslConstants.AUTH_METHOD_NAME;
import static org.apache.pulsar.common.sasl.SaslConstants.JAAS_CLIENT_SECTION_NAME;
import static org.apache.pulsar.common.sasl.SaslConstants.JAAS_DEFAULT_CLIENT_SECTION_NAME;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN_EXPIRED;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_BROKER_PROTOCOL;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_STATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_TYPE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_SERVER_TYPE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_CLIENT_INIT;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_COMPLETE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_NEGOTIATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER_CHECK_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_TYPE_VALUE;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.security.auth.login.LoginException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.client.impl.auth.PulsarSaslClient.ClientCallbackHandler;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.JAASCredentialsContainer;

/**
 * Authentication provider for SASL based authentication.
 *
 * SASL need config files through JVM parameter:
 *   a jaas.conf, which is set by `-Djava.security.auth.login.config=/dir/jaas.conf`
 *   for Kerberos a krb5.conf, which is set by `-Djava.security.krb5.conf=/dir/krb5.conf`
 */
@Slf4j
public class AuthenticationSasl implements Authentication, EncodedAuthenticationParameterSupport {
    private static final long serialVersionUID = 1L;
    // this is a static object that shares amongst client.
    private static JAASCredentialsContainer jaasCredentialsContainer;
    private static volatile boolean initializedJAAS = false;

    private Map<String, String> configuration;
    private String loginContextName;
    private String serverType = null;

    public AuthenticationSasl() {
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_METHOD_NAME;
    }

    @Override
    public AuthenticationDataProvider getAuthData(String serverHostname) throws PulsarClientException {
        // reuse this to return a DataProvider which contains a SASL client
        try {
            PulsarSaslClient saslClient = new PulsarSaslClient(serverHostname, serverType,
                    jaasCredentialsContainer.getSubject());
            return new SaslAuthenticationDataProvider(saslClient);
        } catch (Throwable t) {
            log.error("Failed create sasl client", t);
            throw new PulsarClientException(t);
        }
    }

    @Override
    public void configure(String encodedAuthParamString) {
        if (isBlank(encodedAuthParamString)) {
            log.info("authParams for SASL is be empty, will use default JAAS client section name: {}",
                JAAS_DEFAULT_CLIENT_SECTION_NAME);
        }

        try {
            setAuthParams(AuthenticationUtil.configureFromJsonString(encodedAuthParamString));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse SASL authParams", e);
        }
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
        try {
            setAuthParams(authParams);
        }  catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse SASL authParams", e);
        }
    }

    // use passed in parameter to config ange get jaasCredentialsContainer.
    private void setAuthParams(Map<String, String> authParams) throws PulsarClientException {
        this.configuration = authParams;

        // read section from config files of kerberos
        this.loginContextName = authParams
            .getOrDefault(JAAS_CLIENT_SECTION_NAME, JAAS_DEFAULT_CLIENT_SECTION_NAME);
        this.serverType = authParams
            .getOrDefault(SASL_SERVER_TYPE, SASL_BROKER_PROTOCOL);

        // init the static jaasCredentialsContainer that shares amongst client.
        if (!initializedJAAS) {
            synchronized (this) {
                if (jaasCredentialsContainer == null) {
                    log.info("JAAS loginContext is: {}.", loginContextName);
                    try {
                        jaasCredentialsContainer = new JAASCredentialsContainer(
                            loginContextName,
                            new ClientCallbackHandler(),
                            configuration);
                        initializedJAAS = true;
                    } catch (LoginException e) {
                        log.error("JAAS login in client failed", e);
                        throw new PulsarClientException(e);
                    }
                }
            }
        }
    }

    @Override
    public void start() throws PulsarClientException {
        client = ClientBuilder.newClient();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    private String saslRoleToken = null;
    private Client client = null;

    // role token exists but expired return true
    private boolean isRoleTokenExpired(Map<String, String> responseHeaders) {
        if ((saslRoleToken != null)
            && (responseHeaders != null)
            // header type match
            && (responseHeaders.get(SASL_HEADER_TYPE) != null && responseHeaders.get(SASL_HEADER_TYPE)
                    .equalsIgnoreCase(SASL_TYPE_VALUE))
            // header state expired
            && (responseHeaders.get(SASL_HEADER_STATE) != null && responseHeaders.get(SASL_HEADER_STATE)
                    .equalsIgnoreCase(SASL_AUTH_ROLE_TOKEN_EXPIRED))) {
            return true;
        } else {
            return false;
        }
    }

    @SneakyThrows(Exception.class)
    private Builder newRequestBuilder(WebTarget target,
                                      AuthenticationDataProvider authData,
                                      Map<String, String> previousResHeaders) {
        Builder builder = target.request(MediaType.APPLICATION_JSON);
        Set<Entry<String, String>>  headers = newRequestHeader(
            target.getUri().toString(),
            authData,
            previousResHeaders);

        headers.forEach(entry -> {
            builder.header(entry.getKey(), entry.getValue());
        });
        return builder;
    }

    // set header according to previous response
    @Override
    public Set<Entry<String, String>> newRequestHeader(String hostName,
                                                       AuthenticationDataProvider authData,
                                                       Map<String, String> previousRespHeaders) throws Exception {

        Map<String, String> headers = new HashMap<>();

        if (authData.hasDataForHttp()) {
            authData.getHttpHeaders().forEach(header ->
                headers.put(header.getKey(), header.getValue())
            );
        }

        // role token expired in last check. remove role token, new sasl client, restart auth.
        if (isRoleTokenExpired(previousRespHeaders)) {
            previousRespHeaders = null;
            saslRoleToken = null;
            authData = getAuthData(hostName);
        }

        // role token is not expired and OK to use.
        // 1. first time request, send server to check if expired.
        // 2. server checked, and return SASL_STATE_COMPLETE, ask server to complete auth
        // 3. server checked, and not return SASL_STATE_COMPLETE
        if (saslRoleToken != null) {
            headers.put(SASL_AUTH_ROLE_TOKEN, saslRoleToken);
            if (previousRespHeaders == null) {
                // first time auth, ask server to check the role token expired or not.
                if (log.isDebugEnabled()) {
                    log.debug("request builder add token: Check token");
                }
                headers.put(SASL_HEADER_STATE, SASL_STATE_SERVER_CHECK_TOKEN);
            } else if (previousRespHeaders.get(SASL_HEADER_STATE).equalsIgnoreCase(SASL_STATE_COMPLETE)) {
                headers.put(SASL_HEADER_STATE, SASL_STATE_COMPLETE);
                if (log.isDebugEnabled()) {
                    log.debug("request builder add token. role verified by server");
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("request builder add token. NOT complete. state: {}",
                        previousRespHeaders.get(SASL_HEADER_STATE));
                }
                headers.put(SASL_HEADER_STATE, SASL_STATE_NEGOTIATE);
            }
            return headers.entrySet();
        }

        // role token is null, need do auth.
        if (previousRespHeaders == null) {
            if (log.isDebugEnabled()) {
                log.debug("Init authn in client side");
            }
            // first time init
            headers.put(SASL_HEADER_STATE, SASL_STATE_CLIENT_INIT);
            AuthData initData = authData.authenticate(AuthData.INIT_AUTH_DATA);
            headers.put(SASL_AUTH_TOKEN,
                Base64.getEncoder().encodeToString(initData.getBytes()));
        } else {
            AuthData brokerData = AuthData.of(
                Base64.getDecoder().decode(
                    previousRespHeaders.get(SASL_AUTH_TOKEN)));
            AuthData clientData = authData.authenticate(brokerData);

            headers.put(SASL_STATE_SERVER, previousRespHeaders.get(SASL_STATE_SERVER));
            headers.put(SASL_HEADER_TYPE, SASL_TYPE_VALUE);
            headers.put(SASL_HEADER_STATE, SASL_STATE_NEGOTIATE);
            headers.put(SASL_AUTH_TOKEN,
                Base64.getEncoder().encodeToString(clientData.getBytes()));
        }

        return headers.entrySet();
    }

    private Map<String, String> getHeaders(Response response) {
        Map<String, String> headers = new HashMap<>();
        String saslHeader = response.getHeaderString(SASL_HEADER_TYPE);
        String headerState = response.getHeaderString(SASL_HEADER_STATE);
        String authToken = response.getHeaderString(SASL_AUTH_TOKEN);
        String serverStateId = response.getHeaderString(SASL_STATE_SERVER);

        if (saslRoleToken != null) {
            headers.put(SASL_AUTH_ROLE_TOKEN, saslRoleToken);
        }

        headers.put(SASL_HEADER_TYPE, saslHeader);
        headers.put(SASL_HEADER_STATE, headerState);
        headers.put(SASL_AUTH_TOKEN, authToken);
        headers.put(SASL_STATE_SERVER, serverStateId);
        return headers;
    }

    @Override
    public void authenticationStage(String requestUrl,
                                    AuthenticationDataProvider authData,
                                    Map<String, String> previousResHeaders,
                                    CompletableFuture<Map<String, String>> authFuture) {
        // a new request for sasl auth
        Builder builder = newRequestBuilder(client.target(requestUrl), authData, previousResHeaders);
        builder.async().get(new InvocationCallback<Response>() {
            @Override
            public void completed(Response response) {
                if (response.getStatus() == HTTP_UNAUTHORIZED) {
                    // sasl auth on going
                    authenticationStage(requestUrl, authData, getHeaders(response), authFuture);
                    return;
                }

                if (response.getStatus() != HttpURLConnection.HTTP_OK) {
                    log.warn("HTTP get request failed: {}", response.getStatusInfo());
                    authFuture.completeExceptionally(new PulsarClientException("Sasl Auth request failed: "
                            + response.getStatus()));
                    return;
                } else {
                    if (response.getHeaderString(SASL_AUTH_ROLE_TOKEN) != null) {
                        saslRoleToken = response.getHeaderString(SASL_AUTH_ROLE_TOKEN);
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Complete auth with saslRoleToken: {}", saslRoleToken);
                    }
                    authFuture.complete(getHeaders(response));
                    return;
                }
            }

            @Override
            public void failed(Throwable throwable) {
                log.warn("Failed to perform http request", throwable);
                authFuture.completeExceptionally(new PulsarClientException(throwable));
                return;
            }
        });
    }
}
