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

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.security.PublicKey;

import javax.naming.AuthenticationException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.yahoo.athenz.auth.token.RoleToken;
import com.yahoo.athenz.zpe.AuthZpeClient;
import org.apache.pulsar.broker.ServiceConfiguration;

public class AuthenticationProviderAthenz implements AuthenticationProvider {

    private static final String DOMAIN_NAME_LIST = "athenzDomainNames";

    private static final String SYS_PROP_DOMAIN_NAME_LIST = "pulsar.athenz.domain.names";
    private static final String SYS_PROP_ALLOWED_OFFSET = "pulsar.athenz.role.token_allowed_offset";

    private List<String> domainNameList = null;
    private int allowedOffset = 30;

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        String domainNames;
        if (config.getProperty(DOMAIN_NAME_LIST) != null) {
            domainNames = (String) config.getProperty(DOMAIN_NAME_LIST);
        } else if (!StringUtils.isEmpty(System.getProperty(SYS_PROP_DOMAIN_NAME_LIST))) {
            domainNames = System.getProperty(SYS_PROP_DOMAIN_NAME_LIST);
        } else {
            throw new IOException("No athenz domain name specified");
        }

        domainNameList = Lists.newArrayList(domainNames.split(","));
        log.info("Supported domain names for athenz: {}", domainNameList);

        if (!StringUtils.isEmpty(System.getProperty(SYS_PROP_ALLOWED_OFFSET))) {
            try {
                allowedOffset = Integer.parseInt(System.getProperty(SYS_PROP_ALLOWED_OFFSET));
            } catch (NumberFormatException e) {
                throw new IOException("Invalid allowed offset for athenz role token verification specified", e);
            }

            if (allowedOffset < 0) {
                throw new IOException("Allowed offset for athenz role token verification must not be negative");
            }
        }

        log.info("Allowed offset for athenz role token verification: {} sec", allowedOffset);
    }

    @Override
    public String getAuthMethodName() {
        return "athenz";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        SocketAddress clientAddress;
        String roleToken;
        try {

            if (authData.hasDataFromPeer()) {
                clientAddress = authData.getPeerAddress();
            } else {
                throw new AuthenticationException("Authentication data source does not have a client address");
            }

            if (authData.hasDataFromCommand()) {
                roleToken = authData.getCommandData();
            } else if (authData.hasDataFromHttp()) {
                roleToken = authData.getHttpHeader(AuthZpeClient.ZPE_TOKEN_HDR);
            } else {
                throw new AuthenticationException("Authentication data source does not have a role token");
            }

            if (roleToken == null) {
                throw new AuthenticationException("Athenz token is null, can't authenticate");
            }
            if (roleToken.isEmpty()) {
                throw new AuthenticationException("Athenz RoleToken is empty, Server is Using Athenz Authentication");
            }
            if (log.isDebugEnabled()) {
                log.debug("Athenz RoleToken : [{}] received from Client: {}", roleToken, clientAddress);
            }

            RoleToken token = new RoleToken(roleToken);

            if (!domainNameList.contains(token.getDomain())) {
                throw new AuthenticationException(
                        String.format("Athenz RoleToken Domain mismatch, Expected: %s, Found: %s", domainNameList.toString(), token.getDomain()));
            }

            // Synchronize for non-thread safe static calls inside athenz library
            synchronized (this) {
                PublicKey ztsPublicKey = AuthZpeClient.getZtsPublicKey(token.getKeyId());

                if (ztsPublicKey == null) {
                    throw new AuthenticationException("Unable to retrieve ZTS Public Key");
                }

                if (token.validate(ztsPublicKey, allowedOffset, false, null)) {
                    log.debug("Athenz Role Token : {}, Authenticated for Client: {}", roleToken, clientAddress);
                    AuthenticationMetrics.authenticateSuccess(getClass().getSimpleName(), getAuthMethodName());
                    return token.getPrincipal();
                } else {
                    throw new AuthenticationException(
                            String.format("Athenz Role Token Not Authenticated from Client: %s", clientAddress));
                }
            }
        } catch (AuthenticationException exception) {
            AuthenticationMetrics.authenticateFailure(getClass().getSimpleName(), getAuthMethodName(), exception.getMessage());
            throw exception;
        }
    }

    @Override
    public void close() throws IOException {
    }

    @VisibleForTesting
    int getAllowedOffset() {
        return this.allowedOffset;
    }

    private static final Logger log = LoggerFactory.getLogger(AuthenticationProviderAthenz.class);
}
