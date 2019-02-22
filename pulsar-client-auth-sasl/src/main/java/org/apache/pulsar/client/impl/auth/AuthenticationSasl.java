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

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.util.Map;
import javax.security.auth.login.LoginException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.client.impl.auth.PulsarSaslClient.ClientCallbackHandler;
import org.apache.pulsar.common.sasl.JAASCredentialsContainer;
import org.apache.pulsar.common.sasl.SaslConstants;

/**
 * Authentication provider for SASL based authentication.
 *
 * SASL need 2 config files through JVM parameter:
 *   a jaas.conf, which is set by `-Djava.security.auth.login.config=/dir/jaas.conf`
 *   a krb5.conf, which is set by `-Djava.security.krb5.conf=/dir/krb5.conf`
 */
@Slf4j
public class AuthenticationSasl implements Authentication, EncodedAuthenticationParameterSupport {
    private static final long serialVersionUID = 1L;
    // this is a static object that shares amongst client.
    static private JAASCredentialsContainer jaasCredentialsContainer;
    static private volatile boolean initializedJAAS = false;

    private Map<String, String> configuration;
    private String loginContextName;

    public AuthenticationSasl() {
    }

    @Override
    public String getAuthMethodName() {
        return SaslConstants.AUTH_METHOD_NAME;
    }

    @Override
    public AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
        // reuse this to return a DataProvider which contains a SASL client
        try {
            PulsarSaslClient saslClient = new PulsarSaslClient(brokerHostName, jaasCredentialsContainer.getSubject());
            return new SaslAuthenticationDataProvider(saslClient);
        } catch (Throwable t) {
            log.error("Failed create sasl client: {}", t);
            throw new PulsarClientException(t);
        }
    }

    @Override
    public void configure(String encodedAuthParamString) {
        if (isBlank(encodedAuthParamString)) {
            log.info("authParams for SASL is be empty, will use default JAAS client section name: {}",
                SaslConstants.JAAS_DEFAULT_CLIENT_SECTION_NAME);
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
            .getOrDefault(SaslConstants.JAAS_CLIENT_SECTION_NAME, SaslConstants.JAAS_DEFAULT_CLIENT_SECTION_NAME);

        // init the static jaasCredentialsContainer that shares amongst client.
        if (!initializedJAAS) {
            synchronized (this) {
                if (jaasCredentialsContainer == null) {
                    log.info("JAAS loginContext is: {}." , loginContextName);
                    try {
                        jaasCredentialsContainer = new JAASCredentialsContainer(
                            loginContextName,
                            new ClientCallbackHandler(),
                            configuration);
                        initializedJAAS = true;
                    } catch (LoginException e) {
                        log.error("JAAS login in client failed: {}" , e);
                        throw new PulsarClientException(e);
                    }
                }
            }
        }
    }

    @Override
    public void start() throws PulsarClientException {

    }

    @Override
    public void close() throws IOException {
    }

}
