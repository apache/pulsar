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
package org.apache.pulsar.broker.authentication.scram;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationProviderList;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.scram.AuthenticationSaslScramImpl;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.scram.HmacRoleTokenSigner;
import org.apache.pulsar.common.sasl.scram.ScramCredential;
import org.apache.pulsar.common.sasl.scram.ScramFormatter;
import org.junit.BeforeClass;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.naming.AuthenticationException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.*;

//@PowerMockIgnore({"javax.net.ssl.*", "javax.management.*", "javax.security.*", "javax.crypto.*",
//        "com.sun.management.OperatingSystemMXBean","org.glassfish.jersey.inject.hk2.Hk2InjectionManagerFactory"})
@PrepareForTest({AuthenticationProviderSaslScramImpl.class, AuthenticationProvider.class, Class.class})
@Slf4j
public class SaslSCRAMAuthenticateTest extends ProducerConsumerBase {

    private static Properties properties;

    private static String localHostname = "localhost";

    private static Authentication authSasl;

    private static String username = "pulsarAdmin";
    private static String password = "0123456789012345678901234567890123456789";
    private static String decryptClass = "";

//    @ObjectFactory
//    public IObjectFactory objectFactory() {
//        return new PowerMockObjectFactory();
//    }

    @BeforeClass
    public static void initTestCase() throws Exception {

    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {

        //prepare user info
        String adminUser = System.getProperty("pulsarSaslAdminUser");
        String adminPassword = System.getProperty("pulsarSaslAdminPassword");
        String adminSCRAMConfig = System.getProperty("pulsarSaslAdminScramConf");

        System.setProperty("pulsarSaslAdminUser", username);
        System.setProperty("pulsarSaslAdminPassword", password);

        ScramCredential credential = new ScramFormatter().generateCredential(password, 10000);

        System.setProperty("pulsarSaslAdminScramConf", ScramFormatter.credentialToString(credential));


//        AuthenticationProviderSaslScramImpl spy = PowerMockito.spy(new AuthenticationProviderSaslScramImpl());
//        Mockito.when(spy.getScramUsers()).thenReturn(scramUsers);
//        c.mockStatic(Class.class);
//        PowerMockito.when((AuthenticationProviderSaslScramImpl) Class.forName(AuthenticationProviderSaslScramImpl.class.getName()).newInstance()).thenReturn(spy);
//        PowerMockito.whenNew(AuthenticationProviderSaslScramImpl.class).withAnyArguments().thenReturn(spy);


        Map<String, String> authParams = Maps.newHashMap();
        authParams.put("saslRole", username);
        authParams.put("roleSecret", password);
        authParams.put("decryptClass", decryptClass);
        authSasl = AuthenticationFactory.create(AuthenticationSaslScramImpl.class.getName(), authParams);


        log.info("-- {} --, start at host: {}", methodName, localHostname);
        // use http lookup to verify HttpClient works well.
        isTcpLookup = false;

        conf.setAdvertisedAddress(localHostname);
        conf.setAuthenticationEnabled(true);
        conf.setSaslJaasClientAllowedIds(".*");
        conf.setSaslJaasServerSectionName("PulsarBroker");
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderSaslScramImpl.class.getName());
        conf.setAuthenticationProviders(providers);
        conf.setClusterName("test");
        conf.setSuperUserRoles(ImmutableSet.of(username));

        super.init();

        lookupUrl = new URI(pulsar.getWebServiceAddress());

        replacePulsarClient(PulsarClient.builder()
                .serviceUrl(lookupUrl.toString())
                .statsInterval(0, TimeUnit.SECONDS)
                .authentication(authSasl));

        // set admin auth, to verify admin web resources

        log.info("set client jaas section name: PulsarClient");
        admin = PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl.toString())
                .authentication(authSasl)
                .build();
        log.info("-- {} --, end.", methodName);

        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    // Test could verify with kerberos configured.
    @Test
    public void testProducerAndConsumerPassed() throws Exception {
        log.info("-- {} -- start", methodName);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://my-property/my-ns/my-topic")
                .subscriptionName("my-subscriber-name")
                .subscribe();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic("persistent://my-property/my-ns/my-topic")
                .enableBatching(false);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
            log.info("Produced message: [{}]", message);
        }

        Message<byte[]> msg = null;
        Set<String> messageSet = Sets.newHashSet();
        for (int i = 0; i < 10; i++) {
            msg = consumer.receive(5, TimeUnit.SECONDS);
            String receivedMessage = new String(msg.getData());
            log.info("Received message: [{}]", receivedMessage);
            String expectedMessage = "my-message-" + i;
            testMessageOrderAndDuplicates(messageSet, receivedMessage, expectedMessage);
        }
        // Acknowledge the consumption of all messages at once
        consumer.acknowledgeCumulative(msg);
        consumer.close();

        log.info("-- {} -- end", methodName);
    }

    // Test sasl server/client auth.
    @Test
    public void testSaslServerAndClientAuth() throws Exception {
        log.info("-- {} -- start", methodName);
        String hostName = "localhost";

        // prepare client and server side resource
        AuthenticationDataProvider dataProvider = authSasl.getAuthData(hostName);

        AuthenticationProviderList providerList = (AuthenticationProviderList)
                (pulsar.getBrokerService().getAuthenticationService()
                        .getAuthenticationProvider("scram"));
        AuthenticationProviderSaslScramImpl saslServer =
                (AuthenticationProviderSaslScramImpl) providerList.getProviders().get(0);


        // auth between server and client.
        // first time auth
        AuthData initData1 = dataProvider.authenticate(AuthData.INIT_AUTH_DATA);

        SocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 1234);
        AuthenticationState authState = saslServer.newAuthState(initData1, clientAddress, null);
        System.out.println("client data " + new String(initData1.getBytes(), UTF_8));

        AuthData serverData1 = authState.authenticate(initData1);

        System.out.println("server data " + new String(serverData1.getBytes(), UTF_8));
        boolean complete = authState.isComplete();
        assertFalse(complete);

        // second time auth, completed
        AuthData initData2 = dataProvider.authenticate(serverData1);

        System.out.println("client data " + new String(initData2.getBytes(), UTF_8));

        AuthData serverData2 = authState.authenticate(initData2);


        System.out.println("server data " + new String(serverData2.getBytes(), UTF_8));


        // second time auth, completed
        AuthData initData3 = dataProvider.authenticate(serverData2);

        System.out.println("client data " + new String(initData3.getBytes(), UTF_8));

        AuthData serverData3 = authState.authenticate(initData3);


        complete = authState.isComplete();

        assertTrue(complete);
        assertNull(serverData3);

        // if completed, server could not auth again.
        try {
            authState.authenticate(initData2);
            fail("Expected fail because auth completed for authState");
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("-- {} -- end", methodName);
    }


    @Test
    public void testSaslServerAndClientAuthInvalidNonce() throws Exception {
        log.info("-- {} -- start", methodName);
        String hostName = "localhost";

        // prepare client and server side resource
        AuthenticationDataProvider clientDataProvider = authSasl.getAuthData(hostName);

        AuthenticationProviderList providerList = (AuthenticationProviderList)
                (pulsar.getBrokerService().getAuthenticationService()
                        .getAuthenticationProvider("scram"));
        AuthenticationProviderSaslScramImpl saslServer =
                (AuthenticationProviderSaslScramImpl) providerList.getProviders().get(0);


        // auth between server and client.
        // first time auth
        AuthData initData1 = clientDataProvider.authenticate(AuthData.INIT_AUTH_DATA);
        //n,,n=pulsarAdmin,r=7af38d4d-95a5-4829-9082-77922b76bb04
        //change client to server data
        initData1 = AuthData.of("n,,n=pulsarAdmin,r=7af38d4d-95a5-4829-9082-77922b76bb04".getBytes(UTF_8));

        SocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 1234);
        AuthenticationState serverAuthState = saslServer.newAuthState(initData1, clientAddress, null);

        System.out.println("client data " + new String(initData1.getBytes(), UTF_8));
        //server auth invalid data and return invalid nonce data r=7af38d4d-95a5-4829-9082-77922b76bb0416f594b8-ad1e-4dd1-a223-70fba60a34a4,s=dHp0b2R1c2lxaHE3YXprcm8zaDV3dWl0eA==,i=10000
        AuthData serverData1 = serverAuthState.authenticate(initData1);

        System.out.println("server data " + new String(serverData1.getBytes(), UTF_8));

        AuthData initData2 = null;
        try {
            initData2 = clientDataProvider.authenticate(serverData1);
            fail("invalid");
        } catch (AuthenticationException e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains("invalid nonce"));
            boolean complete = serverAuthState.isComplete();
            assertFalse(complete);
        }

        log.info("-- {} -- end", methodName);
    }


    @Test
    public void testSaslHmacAdminAuth() throws Exception {
        log.info("-- {} -- start", methodName);
        String hostName = "localhost";

        // prepare client and server side resource
        AuthenticationDataProvider dataProvider = authSasl.getAuthData(hostName);

        AuthenticationProviderList providerList = (AuthenticationProviderList)
                (pulsar.getBrokerService().getAuthenticationService()
                        .getAuthenticationProvider("scram"));
        AuthenticationProviderSaslScramImpl saslServer =
                (AuthenticationProviderSaslScramImpl) providerList.getProviders().get(0);


        AuthenticationDataSource spy = Mockito.spy(new AuthenticationDataSource() {
        });
        SocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 1234);
        Mockito.when(spy.hasDataFromHttp()).thenReturn(true);
        Mockito.when(spy.getPeerAddress()).thenReturn(clientAddress);


//        "u=pulsarAdmin&i=scram&e=1616416871193&s=C4681422F1C98DB5C02F7E5812D54E61AB5219841CC1BA3ECEF89E31FA60105"
        String signval = "u=" + username + "&i=scram&e=" + (System.currentTimeMillis() + 300000);
        String sg = new HmacRoleTokenSigner(password).sign(signval);
        Mockito.when(spy.getHttpHeader("HmacAuthRoleToken")).thenReturn(sg);


        try {
            String role = saslServer.authenticate(spy);
            assertTrue(role.equals(username));
            System.out.println(role);
        } catch (Exception e) {
            e.printStackTrace();
            fail("invalid");
        }

        log.info("-- {} -- end", methodName);
    }


    @Test
    public void testSaslHmacAdminAuthExpired() throws Exception {
        log.info("-- {} -- start", methodName);
        String hostName = "localhost";

        // prepare client and server side resource
        AuthenticationDataProvider dataProvider = authSasl.getAuthData(hostName);

        AuthenticationProviderList providerList = (AuthenticationProviderList)
                (pulsar.getBrokerService().getAuthenticationService()
                        .getAuthenticationProvider("scram"));
        AuthenticationProviderSaslScramImpl saslServer =
                (AuthenticationProviderSaslScramImpl) providerList.getProviders().get(0);


        AuthenticationDataSource spy = Mockito.spy(new AuthenticationDataSource() {
        });
        SocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 1234);
        Mockito.when(spy.hasDataFromHttp()).thenReturn(true);
        Mockito.when(spy.getPeerAddress()).thenReturn(clientAddress);


//        "u=pulsarAdmin&i=scram&e=1616416871193&s=C4681422F1C98DB5C02F7E5812D54E61AB5219841CC1BA3ECEF89E31FA60105"
        String signval = "u=" + username + "&i=scram&e=" + (System.currentTimeMillis() - 300000);
        String sg = new HmacRoleTokenSigner(password).sign(signval);
        Mockito.when(spy.getHttpHeader("HmacAuthRoleToken")).thenReturn(sg);

        try {
            String role = saslServer.authenticate(spy);
            fail("invalid");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("token expired"));
        }

        log.info("-- {} -- end", methodName);
    }

    @Test
    public void testSaslHmacAdminAuthExpiredToolong() throws Exception {
        log.info("-- {} -- start", methodName);
        String hostName = "localhost";

        // prepare client and server side resource
        AuthenticationDataProvider dataProvider = authSasl.getAuthData(hostName);

        AuthenticationProviderList providerList = (AuthenticationProviderList)
                (pulsar.getBrokerService().getAuthenticationService()
                        .getAuthenticationProvider("scram"));
        AuthenticationProviderSaslScramImpl saslServer =
                (AuthenticationProviderSaslScramImpl) providerList.getProviders().get(0);


        AuthenticationDataSource spy = Mockito.spy(new AuthenticationDataSource() {
        });
        SocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 1234);
        Mockito.when(spy.hasDataFromHttp()).thenReturn(true);
        Mockito.when(spy.getPeerAddress()).thenReturn(clientAddress);


//        "u=pulsarAdmin&i=scram&e=1616416871193&s=C4681422F1C98DB5C02F7E5812D54E61AB5219841CC1BA3ECEF89E31FA60105"
        String signval = "u=" + username + "&i=scram&e=" + (System.currentTimeMillis() + 3700000);
        String sg = new HmacRoleTokenSigner(password).sign(signval);
        Mockito.when(spy.getHttpHeader("HmacAuthRoleToken")).thenReturn(sg);

        try {
            String role = saslServer.authenticate(spy);
            fail("invalid");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("token expired"));
        }

        log.info("-- {} -- end", methodName);
    }

    @Test
    public void testSaslHmacAdminAuthInvalidUser() throws Exception {
        log.info("-- {} -- start", methodName);
        String hostName = "localhost";

        // prepare client and server side resource
        AuthenticationDataProvider dataProvider = authSasl.getAuthData(hostName);

        AuthenticationProviderList providerList = (AuthenticationProviderList)
                (pulsar.getBrokerService().getAuthenticationService()
                        .getAuthenticationProvider("scram"));
        AuthenticationProviderSaslScramImpl saslServer =
                (AuthenticationProviderSaslScramImpl) providerList.getProviders().get(0);


        AuthenticationDataSource spy = Mockito.spy(new AuthenticationDataSource() {
        });
        SocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 1234);
        Mockito.when(spy.hasDataFromHttp()).thenReturn(true);
        Mockito.when(spy.getPeerAddress()).thenReturn(clientAddress);

        String username = "abc";

//        "u=pulsarAdmin&i=scram&e=1616416871193&s=C4681422F1C98DB5C02F7E5812D54E61AB5219841CC1BA3ECEF89E31FA60105"
        String signval = "u=" + username + "&i=scram&e=" + (System.currentTimeMillis() + 300000);
        String sg = new HmacRoleTokenSigner(password).sign(signval);
        Mockito.when(spy.getHttpHeader("HmacAuthRoleToken")).thenReturn(sg);

        try {
            String role = saslServer.authenticate(spy);
            fail("invalid");
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains("Invalid user"));
        }

        log.info("-- {} -- end", methodName);
    }

    @Test
    public void testSaslHmacAdminAuthInvalidSignValue() throws Exception {
        log.info("-- {} -- start", methodName);
        String hostName = "localhost";

        // prepare client and server side resource
        AuthenticationDataProvider dataProvider = authSasl.getAuthData(hostName);

        AuthenticationProviderList providerList = (AuthenticationProviderList)
                (pulsar.getBrokerService().getAuthenticationService()
                        .getAuthenticationProvider("scram"));
        AuthenticationProviderSaslScramImpl saslServer =
                (AuthenticationProviderSaslScramImpl) providerList.getProviders().get(0);


        AuthenticationDataSource spy = Mockito.spy(new AuthenticationDataSource() {
        });
        SocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 1234);
        Mockito.when(spy.hasDataFromHttp()).thenReturn(true);
        Mockito.when(spy.getPeerAddress()).thenReturn(clientAddress);


//        "u=pulsarAdmin&i=scram&e=1616416871193&s=C4681422F1C98DB5C02F7E5812D54E61AB5219841CC1BA3ECEF89E31FA60105"
        String signval = "u=" + username + "&i=scram&e=" + (System.currentTimeMillis() + 300000);
//        String sg = new HmacRoleTokenSigner(password).sign(signval);
        String sg = signval + "&s=xxx";
        Mockito.when(spy.getHttpHeader("HmacAuthRoleToken")).thenReturn(sg);

        try {
            String role = saslServer.authenticate(spy);
            fail("invalid");
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains("Invalid signature"));
        }

        log.info("-- {} -- end", methodName);
    }

    @Test
    public void testSaslHmacAdminAuthInvalidSignFormat() throws Exception {
        log.info("-- {} -- start", methodName);
        String hostName = "localhost";

        // prepare client and server side resource
        AuthenticationDataProvider dataProvider = authSasl.getAuthData(hostName);

        AuthenticationProviderList providerList = (AuthenticationProviderList)
                (pulsar.getBrokerService().getAuthenticationService()
                        .getAuthenticationProvider("scram"));
        AuthenticationProviderSaslScramImpl saslServer =
                (AuthenticationProviderSaslScramImpl) providerList.getProviders().get(0);


        AuthenticationDataSource spy = Mockito.spy(new AuthenticationDataSource() {
        });
        SocketAddress clientAddress = new InetSocketAddress("127.0.0.1", 1234);
        Mockito.when(spy.hasDataFromHttp()).thenReturn(true);
        Mockito.when(spy.getPeerAddress()).thenReturn(clientAddress);


//        "u=pulsarAdmin&i=scram&e=1616416871193&s=C4681422F1C98DB5C02F7E5812D54E61AB5219841CC1BA3ECEF89E31FA60105"
//        String signval = "u=" + username + "&i=scram&e=" + (System.currentTimeMillis() + 300000);
//        String sg = new HmacRoleTokenSigner(password).sign(signval);
//        String sg = signval + "&s=xxx";
        Mockito.when(spy.getHttpHeader("HmacAuthRoleToken")).thenReturn("i am a invalid sign");

        try {
            String role = saslServer.authenticate(spy);
            fail("invalid");
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains("Invalid format signed text"));
        }

        log.info("-- {} -- end", methodName);
    }
}
