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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.Configuration;

import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationSasl;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.sasl.SaslConstants;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class SaslAuthenticateTest extends ProducerConsumerBase {
    public static File kdcDir;
    public static File kerberosWorkDir;
    public static File secretKeyFile;

    private static MiniKdc kdc;
    private static Properties properties;

    private static String localHostname = "localhost";
    private static Authentication authSasl;

    @BeforeClass
    public static void startMiniKdc() throws Exception {
        kdcDir = Files.createTempDirectory("test-kdc-dir").toFile();
        kerberosWorkDir = Files.createTempDirectory("test-kerberos-work-dir").toFile();

        properties = MiniKdc.createConf();
        kdc = new MiniKdc(properties, kdcDir);
        kdc.start();

        String principalServerNoRealm = "broker/" + localHostname;
        String principalServer = "broker/" + localHostname + "@" + kdc.getRealm();
        log.info("principalServer: " + principalServer);
        String principalClientNoRealm = "client";
        String principalClient = principalClientNoRealm + "@" + kdc.getRealm();

        log.info("principalClient: " + principalClient);

        File keytabClient = new File(kerberosWorkDir, "pulsarclient.keytab");
        kdc.createPrincipal(keytabClient, principalClientNoRealm);

        File keytabServer = new File(kerberosWorkDir, "pulsarbroker.keytab");
        kdc.createPrincipal(keytabServer, principalServerNoRealm);

        File jaasFile = new File(kerberosWorkDir, "jaas.conf");
        try (FileWriter writer = new FileWriter(jaasFile)) {
            writer.write("\n"
                + "PulsarBroker {\n"
                + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                + "  useKeyTab=true\n"
                + "  keyTab=\"" + keytabServer.getAbsolutePath() + "\n"
                + "  storeKey=true\n"
                + "  useTicketCache=false\n" // won't test useTicketCache=true on JUnit tests
                + "  principal=\"" + principalServer + "\";\n"
                + "};\n"
                + "\n"
                + "\n"
                + "\n"
                + "PulsarClient {\n"
                + "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
                + "  useKeyTab=true\n"
                + "  keyTab=\"" + keytabClient.getAbsolutePath() + "\n"
                + "  storeKey=true\n"
                + "  useTicketCache=false\n"
                + "  principal=\"" + principalClient + "\";\n"
                + "};\n"
            );
        }

        File krb5file = new File(kerberosWorkDir, "krb5.conf");
        try (FileWriter writer = new FileWriter(krb5file)) {
            String conf = "[libdefaults]\n"
                + " default_realm = " + kdc.getRealm() + "\n"
                + " udp_preference_limit = 1\n" // force use TCP
                + "\n"
                + "\n"
                + "[realms]\n"
                + " " + kdc.getRealm() + "  = {\n"
                + "  kdc = " + kdc.getHost() + ":" + kdc.getPort() + "\n"
                + " }";
            writer.write(conf);
            log.info("krb5.conf:\n" + conf);
        }

        System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
        System.setProperty("java.security.krb5.conf", krb5file.getAbsolutePath());
        Configuration.getConfiguration().refresh();

        // Client config
        Map<String, String> clientSaslConfig = Maps.newHashMap();
        clientSaslConfig.put("saslJaasClientSectionName", "PulsarClient");
        clientSaslConfig.put("serverType", "broker");
        log.info("set client jaas section name: PulsarClient");
        authSasl = AuthenticationFactory.create(AuthenticationSasl.class.getName(), clientSaslConfig);
        log.info("created AuthenticationSasl");
    }

    @AfterClass(alwaysRun = true)
    public static void stopMiniKdc() {
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("java.security.krb5.conf");
        if (kdc != null) {
            kdc.stop();
        }
        FileUtils.deleteQuietly(kdcDir);
        FileUtils.deleteQuietly(kerberosWorkDir);
        assertFalse(kdcDir.exists());
        assertFalse(kerberosWorkDir.exists());
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        log.info("-- {} --, start at host: {}", methodName, localHostname);
        // use http lookup to verify HttpClient works well.
        isTcpLookup = false;

        conf.setAdvertisedAddress(localHostname);
        conf.setAuthenticationEnabled(true);
        conf.setSaslJaasClientAllowedIds(".*" + "client" + ".*");
        conf.setSaslJaasServerSectionName("PulsarBroker");
        secretKeyFile = File.createTempFile("saslRoleTokenSignerSecret", ".key");
        Files.write(Paths.get(secretKeyFile.toString()), "PulsarSecret".getBytes());
        conf.setSaslJaasServerRoleTokenSignerSecretPath(secretKeyFile.toString());
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderSasl.class.getName());
        conf.setAuthenticationProviders(providers);
        conf.setClusterName("test");
        conf.setSuperUserRoles(ImmutableSet.of("client" + "@" + kdc.getRealm()));

        super.init();

        lookupUrl = new URI(pulsar.getWebServiceAddress());

        replacePulsarClient(PulsarClient.builder()
            .serviceUrl(lookupUrl.toString())
            .statsInterval(0, TimeUnit.SECONDS)
            .authentication(authSasl));

        // set admin auth, to verify admin web resources
        Map<String, String> clientSaslConfig = Maps.newHashMap();
        clientSaslConfig.put("saslJaasClientSectionName", "PulsarClient");
        clientSaslConfig.put("serverType", "broker");
        log.info("set client jaas section name: PulsarClient");
        admin = PulsarAdmin.builder()
            .serviceHttpUrl(brokerUrl.toString())
            .authentication(AuthenticationFactory.create(AuthenticationSasl.class.getName(), clientSaslConfig))
            .build();
        log.info("-- {} --, end.", methodName);

        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        FileUtils.deleteQuietly(secretKeyFile);
        Assert.assertFalse(secretKeyFile.exists());
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
        Set<String> messageSet = new HashSet<>();
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
        AuthenticationDataProvider dataProvider =  authSasl.getAuthData(hostName);
        AuthenticationProviderList providerList = (AuthenticationProviderList)
            (pulsar.getBrokerService().getAuthenticationService()
                .getAuthenticationProvider(SaslConstants.AUTH_METHOD_NAME));
        AuthenticationProviderSasl saslServer =
            (AuthenticationProviderSasl) providerList.getProviders().get(0);
        AuthenticationState authState = saslServer.newAuthState(null, null, null);

        // auth between server and client.
        // first time auth
        AuthData initData1 = dataProvider.authenticate(AuthData.INIT_AUTH_DATA);
        AuthData serverData1 = authState.authenticate(initData1);
        boolean complete = authState.isComplete();
        assertFalse(complete);

        // second time auth, completed
        AuthData initData2 = dataProvider.authenticate(serverData1);
        AuthData serverData2 = authState.authenticate(initData2);
        complete = authState.isComplete();
        assertTrue(complete);
        assertNull(serverData2.getBytes());

        // if completed, server could not auth again.
        try {
            authState.authenticate(initData2);
            fail("Expected fail because auth completed for authState");
        } catch (Exception e) {
            // expected
        }

        // another server could not serve old client
        try {
            AuthenticationState authState2 = saslServer.newAuthState(null, null, null);
            AuthData serverData3 = authState2.authenticate(initData1);
            fail("Expected fail. server is auth old client data");
        } catch (Exception e) {
            // expected
        }

        log.info("-- {} -- end", methodName);
    }

}
