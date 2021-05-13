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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
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

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test Mutual Authentication.
 * Test connect set success, and producer consumer works well.
 */
@Test(groups = "broker-api")
public class MutualAuthenticationTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(MutualAuthenticationTest.class);

    private MutualAuthentication mutualAuth;

    private static final String[] clientAuthStrings = {
        "MutualClientAuthInit", // step 0
        "MutualClientStep1"     // step 1
    };

    private static final String[] serverAuthStrings = {
        "ResponseMutualClientAuthInit", // step 0
    };

    public static class MutualAuthenticationDataProvider implements AuthenticationDataProvider {
        @Override
        public boolean hasDataFromCommand() {
            return true;
        }

        @Override
        public AuthData authenticate(AuthData data) throws AuthenticationException {
            String dataString = new String(data.getBytes(), UTF_8);
            AuthData toSend;

            if (Arrays.equals(dataString.getBytes(), AuthData.INIT_AUTH_DATA_BYTES)) {
                toSend = AuthData.of(clientAuthStrings[0].getBytes(UTF_8));
            } else if (Arrays.equals(dataString.getBytes(), serverAuthStrings[0].getBytes(UTF_8))) {
                toSend = AuthData.of(clientAuthStrings[1].getBytes(UTF_8));
            } else {
                throw new AuthenticationException();
            }

            log.debug("authenticate in client. passed in :{}, send: {}",
                dataString, new String(toSend.getBytes(), UTF_8));
            return toSend;
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
        private boolean isComplete = false;

        @Override
        public String getAuthRole() throws AuthenticationException {
            return "admin";
        }

        @Override
        public AuthData authenticate(AuthData authData) throws AuthenticationException {
            String dataString = new String(authData.getBytes(), UTF_8);
            AuthData toSend;

            if (Arrays.equals(dataString.getBytes(), clientAuthStrings[0].getBytes(UTF_8))) {
                toSend = AuthData.of(serverAuthStrings[0].getBytes(UTF_8));
            } else if (Arrays.equals(dataString.getBytes(), clientAuthStrings[1].getBytes(UTF_8))) {
                isComplete = true;
                toSend = AuthData.of(null);
            } else {
                throw new AuthenticationException();
            }

            log.debug("authenticate in server. passed in :{}, send: {}",
                dataString, toSend.getBytes() == null ? "null" : new String(toSend.getBytes(), UTF_8));
            return toSend;
        }

        @Override
        public AuthenticationDataSource getAuthDataSource() {
            return null;
        }

        @Override
        public boolean isComplete() {
            return isComplete;
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

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        mutualAuth = new MutualAuthentication();
        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        Set<String> providersClassNames = Sets.newHashSet(MutualAuthenticationProvider.class.getName());
        conf.setAuthenticationProviders(providersClassNames);

        isTcpLookup = true;
        internalSetup();
        producerBaseSetup();
    }

    @Override
    protected void customizeNewPulsarClientBuilder(ClientBuilder clientBuilder) {
        clientBuilder.authentication(mutualAuth);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
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
