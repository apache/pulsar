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

package org.apache.pulsar.common.sasl;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * JAAS Credentials Container.
 * This is added for support Kerberos authentication.
 */
@Slf4j
@Getter
public class JAASCredentialsContainer implements Closeable {
    private Subject subject;
    private String principal;
    private boolean isKrbTicket;
    private boolean isUsingTicketCache;
    private TGTRefreshThread ticketRefreshThread;

    public CallbackHandler callbackHandler;
    private String loginContextName;
    private LoginContext loginContext;
    private Map<String, String> configuration;

    public JAASCredentialsContainer(String loginContextName,
                                    CallbackHandler callbackHandler,
                                    Map<String, String> configuration)
        throws LoginException {
        this.configuration = configuration;
        this.callbackHandler = callbackHandler;
        this.loginContextName = loginContextName;
        AppConfigurationEntry[] entries = Configuration.getConfiguration()
            .getAppConfigurationEntry(loginContextName);
        if (entries == null) {
            final String errorMessage = "loginContext name (JAAS file section header) was null. "
                + "Please check your java.security.login.auth.config (="
                + System.getProperty("java.security.login.auth.config")
                + ") for section header: " + this.loginContextName;
            log.error("No JAAS Configuration section header found for Client: {}", errorMessage);
            throw new LoginException(errorMessage);
        }
        LoginContext loginContext = new LoginContext(loginContextName, callbackHandler);
        loginContext.login();
        log.info("successfully logged in.");

        this.loginContext = loginContext;
        this.subject = loginContext.getSubject();
        this.isKrbTicket = !this.subject.getPrivateCredentials(KerberosTicket.class).isEmpty();
        if (isKrbTicket) {
            this.isUsingTicketCache = SaslConstants.isUsingTicketCache(loginContextName);
            this.principal = SaslConstants.getPrincipal(loginContextName);
            this.ticketRefreshThread = new TGTRefreshThread(this);
        } else {
            throw new LoginException("Kerberos authentication without KerberosTicket provided!");
        }

        ticketRefreshThread.start();
    }

    void setLoginContext(LoginContext login) {
        this.loginContext = login;
    }

    @Override
    public void close() throws IOException {
        if (ticketRefreshThread != null) {
            ticketRefreshThread.interrupt();
            try {
                ticketRefreshThread.join(10000);
            } catch (InterruptedException exit) {
                Thread.currentThread().interrupt();
                if (log.isDebugEnabled()) {
                    log.debug("interrupted while waiting for TGT refresh thread to stop", exit);
                }
            }
        }
    }
}
