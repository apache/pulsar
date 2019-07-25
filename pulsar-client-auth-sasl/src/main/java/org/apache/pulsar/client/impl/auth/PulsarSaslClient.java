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

import static com.google.common.base.Preconditions.checkArgument;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.naming.AuthenticationException;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.KerberosName;
import org.apache.pulsar.common.sasl.SaslConstants;

/**
 * A SASL Client object.
 * This is added for support Kerberos authentication.
 */
@Slf4j
public class PulsarSaslClient {
    private final SaslClient saslClient;
    private final Subject clientSubject;

    public PulsarSaslClient(String serverHostname, String serverType, Subject subject) throws SaslException {
        checkArgument(subject != null, "Cannot create SASL client with NULL JAAS subject");
        checkArgument(!Strings.isNullOrEmpty(serverHostname), "Cannot create SASL client with NUll server name");
        if (!serverType.equals(SaslConstants.SASL_BROKER_PROTOCOL) && !serverType
                                                                           .equals(SaslConstants.SASL_PROXY_PROTOCOL)) {
            log.warn("The server type {} is not recommended", serverType);
        }

        String serverPrincipal = serverType.toLowerCase() + "/" + serverHostname;
        this.clientSubject = subject;
        if (clientSubject.getPrincipals().isEmpty()) {
            throw new SaslException("Cannot create SASL client with empty JAAS subject principal");
        }
        // GSSAPI/Kerberos
        final Object[] principals = clientSubject.getPrincipals().toArray();
        final Principal clientPrincipal = (Principal) principals[0];

        final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
        KerberosName serviceKerberosName = new KerberosName(serverPrincipal + "@" + clientKerberosName.getRealm());
        final String serviceName = serviceKerberosName.getServiceName();
        final String serviceHostname = serviceKerberosName.getHostName();
        final String clientPrincipalName = clientKerberosName.toString();
        log.info("Using JAAS/SASL/GSSAPI auth to connect to server Principal {},",
            serverPrincipal);

        try {
            this.saslClient = Subject.doAs(clientSubject, new PrivilegedExceptionAction<SaslClient>() {
                @Override
                public SaslClient run() throws SaslException {
                    String[] mechs = {"GSSAPI"};
                    return Sasl.createSaslClient(mechs, clientPrincipalName, serviceName, serviceHostname, null,
                        new ClientCallbackHandler());
                }
            });
        } catch (PrivilegedActionException err) {
            log.error("GSSAPI client error", err.getCause());
            throw new SaslException("error while booting GSSAPI client", err.getCause());
        }

        if (saslClient == null) {
            throw new SaslException("Cannot create JVM SASL Client");
        }

    }

    public AuthData evaluateChallenge(final AuthData saslToken) throws AuthenticationException {
        if (saslToken == null) {
            throw new AuthenticationException("saslToken is null");
        }
        try {
            if (clientSubject != null) {
                final byte[] retval = Subject.doAs(clientSubject, new PrivilegedExceptionAction<byte[]>() {
                    @Override
                    public byte[] run() throws SaslException {
                        return saslClient.evaluateChallenge(saslToken.getBytes());
                    }
                });
                return AuthData.of(retval);

            } else {
                return AuthData.of(saslClient.evaluateChallenge(saslToken.getBytes()));
            }
        } catch (Exception e) {
            log.error("SASL error", e.getCause());
            throw new AuthenticationException("SASL/JAAS error" + e.getCause());
        }
    }

    public boolean hasInitialResponse() {
        return saslClient.hasInitialResponse();
    }

    static class ClientCallbackHandler implements CallbackHandler {
        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof AuthorizeCallback) {
                    handleAuthorizeCallback((AuthorizeCallback) callback);
                } else {
                    throw new UnsupportedCallbackException(callback, "Unrecognized SASL GSSAPI Client Callback.");
                }
            }
        }

        private void handleAuthorizeCallback(AuthorizeCallback ac) {
            String authid = ac.getAuthenticationID();
            String authzid = ac.getAuthorizationID();
            if (authid.equals(authzid)) {
                ac.setAuthorized(true);
            } else {
                ac.setAuthorized(false);
            }
            if (ac.isAuthorized()) {
                ac.setAuthorizedID(authzid);
            }
            log.info("Successfully authenticated. authenticationID: {};  authorizationID: {}.",
                authid, authzid);
        }
    }


    public boolean isComplete() {
        return saslClient.isComplete();
    }

}
