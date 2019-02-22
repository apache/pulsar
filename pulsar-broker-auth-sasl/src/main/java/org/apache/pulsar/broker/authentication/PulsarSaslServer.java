/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package org.apache.pulsar.broker.authentication;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.regex.Pattern;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.sasl.KerberosName;
import org.apache.pulsar.common.sasl.SaslConstants;


/**
 * Server side Sasl implementation.
 */
@Slf4j
public class PulsarSaslServer {

    private final SaslServer saslServer;
    private final Pattern allowedIdsPattern;
    private final Subject serverSubject;
    static private final String GSSAPI = "GSSAPI";

    public PulsarSaslServer(Subject subject, Pattern allowedIdsPattern)
        throws IOException, LoginException {
        this.serverSubject = subject;
        this.allowedIdsPattern = allowedIdsPattern;
        saslServer = createSaslServer(serverSubject);
    }

    private SaslServer createSaslServer(final Subject subject)
        throws IOException {
        SaslServerCallbackHandler callbackHandler = new SaslServerCallbackHandler(allowedIdsPattern);
        if (subject.getPrincipals().size() > 0) {
            try {
                final Object[] principals = subject.getPrincipals().toArray();
                final Principal servicePrincipal = (Principal) principals[0];
                if (log.isDebugEnabled()) {
                    log.debug("Authentication will use SASL/JAAS/Kerberos, servicePrincipal is {}", servicePrincipal);
                }

                // e.g. servicePrincipalNameAndHostname := "broker/myhost.foo.com@EXAMPLE.COM"
                final String servicePrincipalNameAndHostname = servicePrincipal.getName();
                int indexOf = servicePrincipalNameAndHostname.indexOf("/");

                // e.g. serviceHostnameAndKerbDomain := "myhost.foo.com@EXAMPLE.COM"
                final String serviceHostnameAndKerbDomain = servicePrincipalNameAndHostname.substring(indexOf + 1,
                    servicePrincipalNameAndHostname.length());
                int indexOfAt = serviceHostnameAndKerbDomain.indexOf("@");

                // Handle Kerberos Service as well as User Principal Names
                final String servicePrincipalName, serviceHostname;
                if (indexOf > 0) {
                    // e.g. servicePrincipalName := "pulsar"
                    servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOf);
                    // e.g. serviceHostname := "myhost.foo.com"
                    serviceHostname = serviceHostnameAndKerbDomain.substring(0, indexOfAt);
                } else {
                    servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOfAt);
                    serviceHostname = null;
                }

                log.info("serviceHostname is '{}', servicePrincipalName is '{}', SASL mechanism(mech) is '{}'.",
                    serviceHostname, servicePrincipalName, GSSAPI);

                try {
                    return Subject.doAs(subject, new PrivilegedExceptionAction<SaslServer>() {
                            @Override
                            public SaslServer run() {
                                try {
                                    SaslServer saslServer;
                                    saslServer = Sasl.createSaslServer(GSSAPI, servicePrincipalName, serviceHostname,
                                        null, callbackHandler);
                                    return saslServer;
                                } catch (SaslException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    );
                } catch (PrivilegedActionException e) {
                    throw new SaslException("error on GSSAPI boot", e.getCause());
                }
            } catch (IndexOutOfBoundsException e) {
                throw new SaslException("error on GSSAPI boot", e);
            }
        } else {
            String errorMessage = "Authentication use SASL/JAAS/GSSAPI but server not have Principals";
            log.error(errorMessage);
            throw new SaslException(errorMessage);
        }
    }

    public boolean isComplete() {
        return saslServer.isComplete();
    }

    /**
     * Reports the authorization ID in effect for the client of this
     * session.
     * This method can only be called if isComplete() returns true.
     * @return The authorization ID of the client.
     * @exception IllegalStateException if this authentication session has not completed
     */
    public String getAuthorizationID() throws IllegalStateException {
        return saslServer.getAuthorizationID();
    }

    public byte[] response(byte[] token) throws SaslException {
        try {
            byte[] retval = saslServer.evaluateResponse(token);
            return retval;
        } catch (SaslException e) {
            log.error("response: Failed to evaluate client token", e);
            throw e;
        }
    }

    static class SaslServerCallbackHandler implements CallbackHandler {
        Pattern allowedIdsPattern;

        public SaslServerCallbackHandler(Pattern pattern) {
            this.allowedIdsPattern = pattern;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof AuthorizeCallback) {
                    handleAuthorizeCallback((AuthorizeCallback) callback);
                } else {
                    throw new UnsupportedCallbackException(callback, "Unrecognized SASL GSSAPI Server Callback.");
                }
            }
        }

        private void handleAuthorizeCallback(AuthorizeCallback ac) {
            String authenticationID = ac.getAuthenticationID();
            String authorizationID = ac.getAuthorizationID();
            if (!authenticationID.equals(authorizationID)) {
                ac.setAuthorized(false);
                log.info("Forbidden access to client: authenticationID: {} is different from authorizationID: {}",
                    authenticationID, authorizationID);
                return;
            }
            if (!allowedIdsPattern.matcher(authenticationID).matches()) {
                ac.setAuthorized(false);
                log.info("Forbidden access to client: authenticationID {}, is not allowed (see {} property).",
                    authenticationID, SaslConstants.JAAS_CLIENT_ALLOWED_IDS);
                return;
            }

            ac.setAuthorized(true);
            log.info("Successfully authenticated client: authenticationID: {};  authorizationID: {}.",
                authenticationID, authorizationID);

            KerberosName kerberosName = new KerberosName(authenticationID);
            try {
                StringBuilder userNameBuilder = new StringBuilder(kerberosName.getShortName());
                userNameBuilder.append("/").append(kerberosName.getHostName());
                userNameBuilder.append("@").append(kerberosName.getRealm());

                log.info("Setting authorizedID: {} ", userNameBuilder);
                ac.setAuthorizedID(userNameBuilder.toString());
            } catch (IOException e) {
                log.error("Failed to set name based on Kerberos authentication rules.");
            }
        }
    }
}
