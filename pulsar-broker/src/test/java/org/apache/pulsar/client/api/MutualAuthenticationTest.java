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
package org.apache.pulsar.client.api;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
import lombok.Getter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Test Mutual Authentication.
 * Test connect set success, and producer consumer works well.
 */
public class MutualAuthenticationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MutualAuthenticationTest.class);

    private MutualAuthentication mutualAuth;

    private static String[] clientAuthStrings = {
        "MutualClientAuthInit", // step 0
        "MutualClientStep1"     // step 1
    };

    private static String[] serverAuthStrings = {
        "ResponseMutualClientAuthInit", // step 0
        "ResponseMutualClientStep1"     // step 1
    };

    public static class MutualAuthenticationDataProvider implements AuthenticationDataProvider {
        private int authStep = 0;

        @Override
        public boolean hasDataFromCommand() {
            return true;
        }

        @Override
        public AuthData authenticate(AuthData data) throws AuthenticationException {
            String dataString = new String(data.getBytes(), Charset.forName("UTF-8"));
            if (!dataString.equalsIgnoreCase("init")) {
                if (!dataString.equals(serverAuthStrings[authStep - 1])) {
                    throw new AuthenticationException();
                }
            }
            log.debug("authenticate in client. passed in :{}, send: clientAuthStrings[{}]: {}",
                dataString, authStep, clientAuthStrings[authStep]);
            return AuthData.of(clientAuthStrings[authStep ++].getBytes());
        }
    }

    public static class MutualAuthentication implements Authentication {
        @Override
        public void close() throws IOException {
            // noop
        }

        @Override
        public String getAuthMethodName() {
            return "MutualAuthentication";
        }

        @Override
        public AuthenticationDataProvider getAuthData(String broker) throws PulsarClientException {
            try {
                return new MutualAuthenticationDataProvider();
            } catch (Exception e) {
                throw new PulsarClientException(e);
            }
        }

        @Override
        public void configure(Map<String, String> authParams) {
            // noop
        }

        @Override
        public void start() throws PulsarClientException {
            // noop
        }
    }


    public static class MutualAuthenticationState implements AuthenticationState {
        private int authStep = 0;

        @Override
        public String getAuthRole() throws AuthenticationException {
            return "admin";
        }

        @Override
        public AuthData authenticate(AuthData authData) throws AuthenticationException {
            String clientData = new String(authData.getBytes(), Charset.forName("UTF-8"));
            if (!clientData.equals(clientAuthStrings[authStep])) {
                throw new AuthenticationException();
            }
            log.debug("authenticate in server. passed in :{}, send: serverAuthStrings[{}]: {}",
                clientData, authStep, serverAuthStrings[authStep]);
            AuthData serverData =  AuthData.of(serverAuthStrings[authStep ++].getBytes());
            return serverData;
        }

        @Override
        public AuthenticationDataSource getAuthDataSource() {
            return null;
        }

        @Override
        public boolean isComplete() {
            return authStep > 1;
        }
    }

    public static class MutualAuthenticationProvider implements AuthenticationProvider {
        @Override
        public void close() throws IOException {
        }

        @Override
        public void initialize(ServiceConfiguration config) throws IOException {
        }

        @Override
        public String getAuthMethodName() {
            return "MutualAuthentication";
        }

        @Override
        public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
            return "admin";
        }

        @Override
        public AuthenticationState newAuthState(AuthData authData,
                                                SocketAddress remoteAddress,
                                                SSLSession sslSession) {
            return new MutualAuthenticationState();
        }
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        mutualAuth = new MutualAuthentication();
        Set<String> superUserRoles = new HashSet<String>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        Set<String> providersClassNames = Sets.newHashSet(MutualAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providersClassNames);

        super.init();
        URI brokerServiceUrl = new URI("pulsar://localhost:" + BROKER_PORT);
        pulsarClient = PulsarClient.builder().serviceUrl(brokerServiceUrl.toString())
            .authentication(mutualAuth)
            .build();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAuthentication() throws Exception {
        log.info("-- Starting {} test --", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://my-property/my-ns/my-topic1")
            .subscriptionName("my-subscriber-name")
            .subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic("persistent://my-property/my-ns/my-topic1")
            .create();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.debug("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        consumer.acknowledgeCumulative(msg);

        log.info("-- Exiting {} test --", methodName);
    }
}
