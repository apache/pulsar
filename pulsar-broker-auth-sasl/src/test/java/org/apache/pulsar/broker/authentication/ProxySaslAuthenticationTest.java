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

import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.Configuration;

import lombok.Cleanup;
import org.apache.commons.io.FileUtils;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.auth.AuthenticationSasl;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProxySaslAuthenticationTest extends ProducerConsumerBase {
	private static final Logger log = LoggerFactory.getLogger(ProxySaslAuthenticationTest.class);

	public static File kdcDir;
	public static File kerberosWorkDir;
	public static File brokerSecretKeyFile;
	public static File proxySecretKeyFile;

	private static MiniKdc kdc;
	private static Properties properties;

	private static String localHostname = "localhost";

	@BeforeClass
	public static void startMiniKdc() throws Exception {
		kdcDir = Files.createTempDirectory("test-kdc-dir").toFile();
		kerberosWorkDir = Files.createTempDirectory("test-kerberos-work-dir").toFile();

		properties = MiniKdc.createConf();
		kdc = new MiniKdc(properties, kdcDir);
		kdc.start();

		String principalBrokerNoRealm = "broker/" + localHostname;
		String principalBroker = "broker/" + localHostname + "@" + kdc.getRealm();
		log.info("principalBroker: " + principalBroker);

		String principalClientNoRealm = "client/" + localHostname;
		String principalClient = principalClientNoRealm + "@" + kdc.getRealm();
		log.info("principalClient: " + principalClient);

		String principalProxyNoRealm = "proxy/" + localHostname;
		String principalProxy = principalProxyNoRealm + "@" + kdc.getRealm();
		log.info("principalProxy: " + principalProxy);

		File keytabClient = new File(kerberosWorkDir, "pulsarclient.keytab");
		kdc.createPrincipal(keytabClient, principalClientNoRealm);

		File keytabBroker = new File(kerberosWorkDir, "pulsarbroker.keytab");
		kdc.createPrincipal(keytabBroker, principalBrokerNoRealm);

		File keytabProxy = new File(kerberosWorkDir, "pulsarproxy.keytab");
		kdc.createPrincipal(keytabProxy, principalProxyNoRealm);

		File jaasFile = new File(kerberosWorkDir, "jaas.conf");
		try (FileWriter writer = new FileWriter(jaasFile)) {
			writer.write("\n"
				+ "PulsarBroker {\n"
				+ "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
				+ "  useKeyTab=true\n"
				+ "  keyTab=\"" + keytabBroker.getAbsolutePath() + "\n"
				+ "  storeKey=true\n"
				+ "  useTicketCache=false\n" // won't test useTicketCache=true on JUnit tests
				+ "  principal=\"" + principalBroker + "\";\n"
				+ "};\n"
				+ "\n"
				+ "\n"
				+ "\n"
				+ "PulsarProxy{\n"
				+ "  com.sun.security.auth.module.Krb5LoginModule required debug=true\n"
				+ "  useKeyTab=true\n"
				+ "  keyTab=\"" + keytabProxy.getAbsolutePath() + "\n"
				+ "  storeKey=true\n"
				+ "  useTicketCache=false\n" // won't test useTicketCache=true on JUnit tests
				+ "  principal=\"" + principalProxy + "\";\n"
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
		Assert.assertFalse(kdcDir.exists());
		Assert.assertFalse(kerberosWorkDir.exists());
	}

	@BeforeMethod
	@Override
	protected void setup() throws Exception {
		log.info("-- {} --, start at host: {}", methodName, localHostname);
		isTcpLookup = true;
		conf.setAdvertisedAddress(localHostname);
		conf.setAuthenticationEnabled(true);
		conf.setSaslJaasClientAllowedIds(".*" + localHostname + ".*");
		conf.setSaslJaasServerSectionName("PulsarBroker");
		brokerSecretKeyFile = File.createTempFile("saslRoleTokenSignerSecret", ".key");
		Files.write(Paths.get(brokerSecretKeyFile.toString()), "PulsarSecret".getBytes());
		conf.setSaslJaasServerRoleTokenSignerSecretPath(brokerSecretKeyFile.toString());
		Set<String> providers = new HashSet<>();
		providers.add(AuthenticationProviderSasl.class.getName());
		conf.setAuthenticationProviders(providers);
		conf.setClusterName("test");
		conf.setSuperUserRoles(ImmutableSet.of("client/" + localHostname + "@" + kdc.getRealm()));

		super.init();

		lookupUrl = new URI(pulsar.getBrokerServiceUrl());

		// set admin auth, to verify admin web resources
		Map<String, String> clientSaslConfig = Maps.newHashMap();
		clientSaslConfig.put("saslJaasClientSectionName", "PulsarClient");
		clientSaslConfig.put("serverType", "broker");
		log.info("set client jaas section name: PulsarClient");
		admin = PulsarAdmin.builder()
			.serviceHttpUrl(brokerUrl.toString())
			.authentication(AuthenticationFactory.create(AuthenticationSasl.class.getName(), clientSaslConfig))
			.build();
		super.producerBaseSetup();
		log.info("-- {} --, end.", methodName);
	}

	@Override
	@AfterMethod(alwaysRun = true)
	protected void cleanup() throws Exception {
		if (brokerSecretKeyFile != null) {
			FileUtils.deleteQuietly(brokerSecretKeyFile);
			Assert.assertFalse(brokerSecretKeyFile.exists());
		}
		if (proxySecretKeyFile != null) {
			FileUtils.deleteQuietly(proxySecretKeyFile);
			Assert.assertFalse(proxySecretKeyFile.exists());
		}
		super.internalCleanup();
	}

	@Test
	void testAuthentication() throws Exception {
		log.info("-- Starting {} test --", methodName);

		// Step 1: Create Admin Client

		// create a client which connects to proxy and pass authData
		String topicName = "persistent://my-property/my-ns/my-topic1";

		ProxyConfiguration proxyConfig = new ProxyConfiguration();
		proxyConfig.setAuthenticationEnabled(true);
		proxyConfig.setServicePort(Optional.of(0));
		proxyConfig.setBrokerProxyAllowedTargetPorts("*");
		proxyConfig.setWebServicePort(Optional.of(0));
		proxyConfig.setBrokerServiceURL(pulsar.getBrokerServiceUrl());
		proxyConfig.setSaslJaasClientAllowedIds(".*" + localHostname + ".*");
		proxyConfig.setSaslJaasServerSectionName("PulsarProxy");

		// proxy connect to broker
		proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationSasl.class.getName());
		proxyConfig.setBrokerClientAuthenticationParameters(
			"{\"saslJaasClientSectionName\": " + "\"PulsarProxy\"," +
				"\"serverType\": " + "\"broker\"}");
		proxySecretKeyFile = File.createTempFile("saslRoleTokenSignerSecret", ".key");
		Files.write(Paths.get(proxySecretKeyFile.toString()), "PulsarSecret".getBytes());
		proxyConfig.setSaslJaasServerRoleTokenSignerSecretPath(proxySecretKeyFile.toString());
		// proxy as a server, it will use sasl to authn
		Set<String> providers = new HashSet<>();
		providers.add(AuthenticationProviderSasl.class.getName());
		proxyConfig.setAuthenticationProviders(providers);

		proxyConfig.setForwardAuthorizationCredentials(true);
		AuthenticationService authenticationService = new AuthenticationService(
                        PulsarConfigurationLoader.convertFrom(proxyConfig));
		ProxyService proxyService = new ProxyService(proxyConfig, authenticationService);

		proxyService.start();
		final String proxyServiceUrl = "pulsar://localhost:" + proxyService.getListenPort().get();
		log.info("1 proxy service started {}", proxyService);

		// Step 3: Pass correct client params
		@Cleanup
		PulsarClient proxyClient = createProxyClient(proxyServiceUrl, 1);
		log.info("2 create proxy client {}, {}", proxyServiceUrl, proxyClient);

		Producer<byte[]> producer = proxyClient.newProducer(Schema.BYTES).topic(topicName).create();
		log.info("3 created producer.");

		Consumer<byte[]> consumer = proxyClient.newConsumer(Schema.BYTES).topic(topicName).subscriptionName("test-sub").subscribe();
		log.info("4 created consumer.");

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

		proxyService.close();
	}

	@Test
	public void testNoErrorEvenIfTheConfigSecretIsEmpty () throws Exception {
		ServiceConfiguration configurationWithoutSecret = Mockito.spy(conf);
		Mockito.doAnswer(invocation -> null).when(configurationWithoutSecret).getSaslJaasServerRoleTokenSignerSecretPath();
		configurationWithoutSecret.setSaslJaasServerRoleTokenSignerSecretPath(null);
		AuthenticationProviderSasl authenticationProviderSasl = new AuthenticationProviderSasl();
		authenticationProviderSasl.initialize(configurationWithoutSecret);
		authenticationProviderSasl.close();
	}

	private PulsarClient createProxyClient(String proxyServiceUrl, int numberOfConnections) throws PulsarClientException {
		Map<String, String> clientSaslConfig = Maps.newHashMap();
		clientSaslConfig.put("saslJaasClientSectionName", "PulsarClient");
		clientSaslConfig.put("serverType", "proxy");
		log.info("set client jaas section name: PulsarClient, serverType: proxy");
		Authentication authSasl = AuthenticationFactory.create(AuthenticationSasl.class.getName(), clientSaslConfig);

		return PulsarClient.builder().serviceUrl(proxyServiceUrl)
				.authentication(authSasl).connectionsPerBroker(numberOfConnections).build();
	}
}
