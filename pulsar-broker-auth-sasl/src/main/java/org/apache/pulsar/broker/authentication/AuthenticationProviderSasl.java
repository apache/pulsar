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

import static org.apache.pulsar.common.sasl.SaslConstants.JAAS_BROKER_SECTION_NAME;
import static org.apache.pulsar.common.sasl.SaslConstants.JAAS_CLIENT_ALLOWED_IDS;
import static org.apache.pulsar.common.sasl.SaslConstants.KINIT_COMMAND;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.naming.AuthenticationException;
import javax.security.auth.login.LoginException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.sasl.JAASCredentialsContainer;
import org.apache.pulsar.common.sasl.SaslConstants;

@Slf4j
public class AuthenticationProviderSasl implements AuthenticationProvider {

    private Pattern allowedIdsPattern;
    private Map<String, String> configuration;

    private JAASCredentialsContainer jaasCredentialsContainer;
    private String loginContextName;

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        this.configuration = Maps.newHashMap();
        final String allowedIdsPatternRegExp = config.getSaslJaasClientAllowedIds();
        configuration.put(JAAS_CLIENT_ALLOWED_IDS, allowedIdsPatternRegExp);
        configuration.put(JAAS_BROKER_SECTION_NAME, config.getSaslJaasBrokerSectionName());
        configuration.put(KINIT_COMMAND, config.getKinitCommand());

        try {
            this.allowedIdsPattern = Pattern.compile(allowedIdsPatternRegExp);
        } catch (PatternSyntaxException error) {
            log.error("Invalid regular expression for id " + allowedIdsPatternRegExp, error);
            throw new IOException(error);
        }

        loginContextName = config.getSaslJaasBrokerSectionName();
        if (jaasCredentialsContainer == null) {
            log.info("JAAS loginContext is: {}." , loginContextName);
            try {
                jaasCredentialsContainer = new JAASCredentialsContainer(
                    loginContextName,
                    new PulsarSaslServer.SaslServerCallbackHandler(allowedIdsPattern),
                    configuration);
            } catch (LoginException e) {
                log.error("JAAS login in broker failed: {}" , e);
                throw new IOException(e);
            }
        }
    }

    @Override
    public AuthenticationDataSource getAuthDataSource() throws IOException {
        try {
            PulsarSaslServer saslServer = new PulsarSaslServer(jaasCredentialsContainer.getSubject(), allowedIdsPattern);
            return new SaslAuthenticationDataSource(saslServer);
        } catch (Throwable t) {
            log.error("Failed create sasl server: {}" , t);
            throw new IOException(t);
        }
    }

    // will get authenticated client id like: "client/host.name.client@EXAMPLE.COM"
    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        if (authData instanceof SaslAuthenticationDataSource) {
            return authData.getAuthorizationID();
        } else {
            throw new AuthenticationException("Not support get authDate from outside sasl.");
        }
        // TODO: for http
        //authData.hasDataFromHttp()
    }

    @Override
    public String getAuthMethodName() {
        return SaslConstants.AUTH_METHOD_NAME;
    }

    @Override
    public void close() throws IOException {
    }

}
