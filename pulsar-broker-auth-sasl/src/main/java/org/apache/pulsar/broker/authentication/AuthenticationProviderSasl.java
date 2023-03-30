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
package org.apache.pulsar.broker.authentication;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.pulsar.broker.web.AuthenticationFilter.AuthenticatedDataAttributeName;
import static org.apache.pulsar.broker.web.AuthenticationFilter.AuthenticatedRoleAttributeName;
import static org.apache.pulsar.common.sasl.SaslConstants.JAAS_CLIENT_ALLOWED_IDS;
import static org.apache.pulsar.common.sasl.SaslConstants.JAAS_SERVER_SECTION_NAME;
import static org.apache.pulsar.common.sasl.SaslConstants.KINIT_COMMAND;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_ROLE_TOKEN_EXPIRED;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_AUTH_TOKEN;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_HEADER_STATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_CLIENT_INIT;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_COMPLETE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_NEGOTIATE;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER;
import static org.apache.pulsar.common.sasl.SaslConstants.SASL_STATE_SERVER_CHECK_TOKEN;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import javax.security.auth.login.LoginException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.JAASCredentialsContainer;
import org.apache.pulsar.common.sasl.SaslConstants;

/**
 * Authentication Provider for SASL (Simple Authentication and Security Layer).
 *
 * Note: This provider does not override the default implementation for
 * {@link AuthenticationProvider#authenticate(AuthenticationDataSource)}. As the Javadoc for the interface's method
 * indicates, this method should only be implemented when using single stage authentication. In the case of this
 * provider, the authentication is multi-stage.
 */
@Slf4j
public class AuthenticationProviderSasl implements AuthenticationProvider {

    private Pattern allowedIdsPattern;
    private Map<String, String> configuration;

    private JAASCredentialsContainer jaasCredentialsContainer;
    private String loginContextName;

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        this.configuration = new HashMap<>();
        final String allowedIdsPatternRegExp = config.getSaslJaasClientAllowedIds();
        configuration.put(JAAS_CLIENT_ALLOWED_IDS, allowedIdsPatternRegExp);
        configuration.put(JAAS_SERVER_SECTION_NAME, config.getSaslJaasServerSectionName());
        configuration.put(KINIT_COMMAND, config.getKinitCommand());

        try {
            this.allowedIdsPattern = Pattern.compile(allowedIdsPatternRegExp);
        } catch (PatternSyntaxException error) {
            log.error("Invalid regular expression for id {}", allowedIdsPatternRegExp, error);
            throw new IOException(error);
        }

        loginContextName = config.getSaslJaasServerSectionName();
        if (jaasCredentialsContainer == null) {
            log.info("JAAS loginContext is: {}.", loginContextName);
            try {
                jaasCredentialsContainer = new JAASCredentialsContainer(
                    loginContextName,
                    new PulsarSaslServer.SaslServerCallbackHandler(allowedIdsPattern),
                    configuration);
            } catch (LoginException e) {
                log.error("JAAS login in broker failed", e);
                throw new IOException(e);
            }
        }
        String saslJaasServerRoleTokenSignerSecretPath = config.getSaslJaasServerRoleTokenSignerSecretPath();
        byte[] secret = null;
        if (StringUtils.isNotBlank(saslJaasServerRoleTokenSignerSecretPath)) {
            secret = readSecretFromUrl(saslJaasServerRoleTokenSignerSecretPath);
        } else {
            secret = Long.toString(new Random().nextLong()).getBytes();
            log.info("JAAS authentication provider using random secret.");
        }
        this.signer = new SaslRoleTokenSigner(secret);
    }

    @Override
    public String getAuthMethodName() {
        return SaslConstants.AUTH_METHOD_NAME;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public AuthenticationState newAuthState(AuthData authData,
                                            SocketAddress remoteAddress,
                                            SSLSession sslSession) throws AuthenticationException {
        try {
            PulsarSaslServer server = new PulsarSaslServer(jaasCredentialsContainer.getSubject(), allowedIdsPattern);
            return new SaslAuthenticationState(server);
        } catch (Throwable t) {
            log.error("Failed create sasl auth state", t);
            throw new AuthenticationException(t.getMessage());
        }
    }

    // for http auth.
    private static final long SASL_ROLE_TOKEN_LIVE_SECONDS = 3600;
    // A signer for http role token, with random secret.
    private SaslRoleTokenSigner signer;

    /**
     * Returns null if authentication has not completed.
     * Return auth role if authentication has completed, and httpRequest's role token contains the authRole
     */
    public String authRoleFromHttpRequest(HttpServletRequest httpRequest) throws AuthenticationException {
        String tokenStr = httpRequest.getHeader(SASL_AUTH_ROLE_TOKEN);

        if (tokenStr == null) {
            return null;
        }

        String unSigned = signer.verifyAndExtract(tokenStr);
        SaslRoleToken token;

        try {
            token = SaslRoleToken.parse(unSigned);
            if (log.isDebugEnabled()) {
                log.debug("server side get role token: {}, session in token:{}, session in request:{}",
                    token, token.getSession(), httpRequest.getRemoteAddr());
            }
        } catch (Exception e) {
            log.error("token parse failed, with exception: ",  e);
            return SASL_AUTH_ROLE_TOKEN_EXPIRED;
        }

        if (!token.isExpired()) {
            return token.getUserRole();
        } else if (token.isExpired()) {
            return SASL_AUTH_ROLE_TOKEN_EXPIRED;
        } else {
            return null;
        }
    }

    private String createAuthRoleToken(String role, String sessionId) {
        long expireAtMs = System.currentTimeMillis() + SASL_ROLE_TOKEN_LIVE_SECONDS * 1000; // 1 hour
        SaslRoleToken token = new SaslRoleToken(role, sessionId, expireAtMs);

        String signed = signer.sign(token.toString());
        if (log.isDebugEnabled()) {
            log.debug("create role token token: {}, role: {} session :{}, expires:{}\nsigned:{}",
                token, token.getUserRole(), token.getSession(), token.getExpires(), signed);
        }
        return signed;
    }

    private byte[] readSecretFromUrl(String secretConfUrl) throws IOException {
        if (secretConfUrl.startsWith("file:")) {
            URI filePath = URI.create(secretConfUrl);
            return Files.readAllBytes(Paths.get(filePath));
        } else if (Files.exists(Paths.get(secretConfUrl))) {
            // Assume the key content was passed in a valid file path
            return Files.readAllBytes(Paths.get(secretConfUrl));
        } else {
            String msg = "Role token signer secret file " + secretConfUrl + " doesn't exist";
            throw new IllegalArgumentException(msg);
        }
    }

    private ConcurrentHashMap<Long, AuthenticationState> authStates = new ConcurrentHashMap<>();

    // return authState if it is in cache.
    private AuthenticationState getAuthState(HttpServletRequest request) {
        String id = request.getHeader(SASL_STATE_SERVER);
        if (id == null) {
            return null;
        }

        try {
            return authStates.get(Long.parseLong(id));
        } catch (NumberFormatException e) {
            log.error("[{}] Wrong Id String in Token {}. e:", request.getRequestURI(),
                id, e);
            return null;
        }
    }

    private void setResponseHeaderState(HttpServletResponse response, String state) {
        response.setHeader(SaslConstants.SASL_HEADER_TYPE, SaslConstants.SASL_TYPE_VALUE);
        response.setHeader(SASL_HEADER_STATE, state);
    }

    /**
     * Passed in request, set response, according to request.
     * and return whether we should do following chain.doFilter or not.
     */
    @Override
    public boolean authenticateHttpRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        AuthenticationState state = getAuthState(request);
        String saslAuthRoleToken = authRoleFromHttpRequest(request);

        // role token exist
        if (saslAuthRoleToken != null) {
            // role token expired, send role token expired to client.
            if (saslAuthRoleToken.equalsIgnoreCase(SASL_AUTH_ROLE_TOKEN_EXPIRED)) {
                setResponseHeaderState(response, SASL_AUTH_ROLE_TOKEN_EXPIRED);
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Role token expired");
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Server side role token expired: {}", request.getRequestURI(), saslAuthRoleToken);
                }
                return false;
            }

            // role token OK to use,
            // if request is ask for role token verify, send auth complete to client
            // if request is a real request with valid role token, pass this request down.
            if (request.getHeader(SASL_HEADER_STATE).equalsIgnoreCase(SASL_STATE_COMPLETE)) {
                request.setAttribute(AuthenticatedRoleAttributeName, saslAuthRoleToken);
                request.setAttribute(AuthenticatedDataAttributeName,
                    new AuthenticationDataHttps(request));
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Server side role token OK to go on: {}", request.getRequestURI(),
                            saslAuthRoleToken);
                }
                return true;
            } else {
                checkState(request.getHeader(SASL_HEADER_STATE).equalsIgnoreCase(SASL_STATE_SERVER_CHECK_TOKEN));
                setResponseHeaderState(response, SASL_STATE_COMPLETE);
                response.setHeader(SASL_STATE_SERVER, request.getHeader(SASL_STATE_SERVER));
                response.setStatus(HttpServletResponse.SC_OK);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Server side role token verified success: {}", request.getRequestURI(),
                            saslAuthRoleToken);
                }
                return false;
            }
        } else {
            // no role token, do sasl auth
            // need new authState
            if (state == null || request.getHeader(SASL_HEADER_STATE).equalsIgnoreCase(SASL_STATE_CLIENT_INIT)) {
                state = newAuthState(null, null, null);
                authStates.put(state.getStateId(), state);
            }
            checkState(request.getHeader(SASL_AUTH_TOKEN) != null,
                "Header token should exist if no role token.");

            // do the sasl auth
            AuthData clientData = AuthData.of(Base64.getDecoder().decode(
                request.getHeader(SASL_AUTH_TOKEN)));
            AuthData brokerData = state.authenticate(clientData);

            // authentication has completed, it has get the auth role.
            if (state.isComplete()) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] SASL server authentication complete, send OK to client.", request.getRequestURI());
                }
                String authRole = state.getAuthRole();
                String authToken = createAuthRoleToken(authRole, String.valueOf(state.getStateId()));
                response.setHeader(SASL_AUTH_ROLE_TOKEN, authToken);

                // auth request complete, return OK, wait for a new real request to come.
                response.setHeader(SASL_STATE_SERVER, String.valueOf(state.getStateId()));
                setResponseHeaderState(response, SASL_STATE_COMPLETE);
                response.setStatus(HttpServletResponse.SC_OK);

                // auth completed, no need to keep authState
                authStates.remove(state.getStateId());
                return false;
            } else {
                // auth not complete
                if (log.isDebugEnabled()) {
                    log.debug("[{}] SASL server authentication not complete, send {} back to client.",
                        request.getRequestURI(), HttpServletResponse.SC_UNAUTHORIZED);
                }
                setResponseHeaderState(response, SASL_STATE_NEGOTIATE);
                response.setHeader(SASL_STATE_SERVER, String.valueOf(state.getStateId()));
                response.setHeader(SASL_AUTH_TOKEN, Base64.getEncoder().encodeToString(brokerData.getBytes()));
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "SASL Authentication not complete.");
                return false;
            }
        }
    }
}
