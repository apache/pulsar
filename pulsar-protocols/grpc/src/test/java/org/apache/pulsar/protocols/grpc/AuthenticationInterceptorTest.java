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
package org.apache.pulsar.protocols.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.StringValue;
import io.grpc.*;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import org.apache.bookkeeper.sasl.MiniKdc;
import org.apache.commons.io.FileUtils;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.*;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.auth.AuthenticationDataBasic;
import org.apache.pulsar.client.impl.auth.AuthenticationSasl;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.protocols.grpc.api.*;
import org.apache.pulsar.protocols.grpc.test.AuthGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.*;

import javax.naming.AuthenticationException;
import javax.security.auth.login.Configuration;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.protocols.grpc.Constants.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

public class AuthenticationInterceptorTest {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationInterceptorTest.class);

    private final String TLS_TRUST_CERT_FILE_PATH = "./src/test/resources/authentication/tls/cacert.pem";
    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/authentication/tls/broker-cert.pem";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/authentication/tls/broker-key.pem";
    private final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    private final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";

    private final String BASIC_CONF_FILE_PATH = "./src/test/resources/authentication/basic/.htpasswd";

    private BrokerService brokerService;
    private AuthenticationService authenticationService;

    private Server server;
    private HmacSigner signer;
    private AuthGrpc.AuthBlockingStub stub;
    public static File kdcDir;
    public static File kerberosWorkDir;

    private static MiniKdc kdc;
    private static Properties properties;

    private static String localHostname = "localhost";
    private static Authentication authSasl;

    @BeforeMethod
    public void setup() throws Exception {
        System.setProperty("pulsar.auth.basic.conf", BASIC_CONF_FILE_PATH);

        brokerService = mock(BrokerService.class);
        authenticationService = mock(AuthenticationService.class);

        doReturn(authenticationService).when(brokerService).getAuthenticationService();
        doReturn(true).when(brokerService).isAuthenticationEnabled();

        signer = new HmacSigner();

        SslContext sslContext = GrpcSslContexts
                .forServer(new File(TLS_SERVER_CERT_FILE_PATH), new File(TLS_SERVER_KEY_FILE_PATH))
                .trustManager(new File(TLS_TRUST_CERT_FILE_PATH))
                .clientAuth(ClientAuth.REQUIRE)
                .build();

        AuthenticationInterceptor authenticationInterceptor = new AuthenticationInterceptor(brokerService, signer);

        server = NettyServerBuilder
                .forAddress(new InetSocketAddress(localHostname, 0))
                .sslContext(sslContext)
                .addService(ServerInterceptors.intercept(
                        new AuthGrpcService(),
                        Collections.singletonList(authenticationInterceptor)
                ))
                .build();

        server.start();

        SslContext clientSslContext = GrpcSslContexts.forClient()
                .trustManager(new File(TLS_TRUST_CERT_FILE_PATH))
                .keyManager(new File(TLS_CLIENT_CERT_FILE_PATH), new File(TLS_CLIENT_KEY_FILE_PATH))
                .build();
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(new InetSocketAddress(localHostname, server.getPort()))
                .negotiationType(NegotiationType.TLS)
                .sslContext(clientSslContext)
                .build();

        stub = AuthGrpc.newBlockingStub(channel);
    }

    @AfterMethod
    public void teardown() throws InterruptedException {
        server.shutdownNow();
        server.awaitTermination(30, TimeUnit.SECONDS);
    }

    @Test
    public void testAuthenticationDisabled() throws Exception {
        doReturn(false).when(brokerService).isAuthenticationEnabled();
        AuthenticationProvider provider = new AuthenticationProviderTls();
        provider.initialize(null);
        doReturn(provider).when(authenticationService).getAuthenticationProvider(provider.getAuthMethodName());

        Metadata headers = new Metadata();
        CommandAuth auth = CommandAuth.newBuilder()
                .setAuthMethod(provider.getAuthMethodName())
                .build();
        headers.put(AUTH_METADATA_KEY, auth.toByteArray());

        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        assertEquals(authStub.getPrincipal(Empty.getDefaultInstance()).getValue(), "unauthenticated user");
    }

    @Test
    public void testAuthenticationBasic() throws IOException {
        AuthenticationProvider provider = new AuthenticationProviderBasic();
        provider.initialize(null);
        doReturn(provider).when(authenticationService).getAuthenticationProvider(provider.getAuthMethodName());

        AuthenticationDataProvider clientData = new AuthenticationDataBasic("superUser", "supepass");
        Metadata headers = new Metadata();
        CommandAuth auth = CommandAuth.newBuilder()
                .setAuthMethod(provider.getAuthMethodName())
                .setAuthData(ByteString.copyFromUtf8(clientData.getCommandData()))
                .build();
        headers.put(AUTH_METADATA_KEY, auth.toByteArray());

        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        assertEquals(authStub.getPrincipal(Empty.getDefaultInstance()).getValue(), "superUser");
    }

    @Test
    public void testAuthenticationTLS() throws Exception {
        AuthenticationProvider provider = new AuthenticationProviderTls();
        provider.initialize(null);
        doReturn(provider).when(authenticationService).getAuthenticationProvider(provider.getAuthMethodName());

        Metadata headers = new Metadata();
        CommandAuth auth = CommandAuth.newBuilder()
                .setAuthMethod(provider.getAuthMethodName())
                .build();
        headers.put(AUTH_METADATA_KEY, auth.toByteArray());

        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        assertEquals(authStub.getPrincipal(Empty.getDefaultInstance()).getValue(), "superUser");
    }

    @Test
    public void testAuthenticationAnonymous() throws Exception {
        doReturn(Optional.of("anonymous")).when(authenticationService).getAnonymousUserRole();

        Metadata headers = new Metadata();
        CommandAuth auth = CommandAuth.newBuilder().build();
        headers.put(AUTH_METADATA_KEY, auth.toByteArray());

        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        assertEquals(authStub.getPrincipal(Empty.getDefaultInstance()).getValue(), "anonymous");
    }

    @Test
    public void testAuthenticationNoAnonymousUser() throws Exception {
        doReturn(Optional.empty()).when(authenticationService).getAnonymousUserRole();

        Metadata headers = new Metadata();
        CommandAuth auth = CommandAuth.newBuilder().build();
        headers.put(AUTH_METADATA_KEY, auth.toByteArray());

        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        try {
            authStub.getPrincipal(Empty.getDefaultInstance());
            fail("Should have thrown UNAUTHENTICATED exception");
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
        }
    }

    @Test
    public void testAuthenticationRoleToken() throws Exception {
        AuthRoleTokenInfo role = AuthRoleTokenInfo.newBuilder()
                .setRole("role-token")
                .setExpires(System.currentTimeMillis() + 3600*1000)
                .build();

        byte[] signature = signer.computeSignature(role.toByteArray());

        AuthRoleToken token = AuthRoleToken.newBuilder()
                .setRoleInfo(role)
                .setSignature(ByteString.copyFrom(signature))
                .build();

        Metadata headers = new Metadata();
        headers.put(AUTH_ROLE_TOKEN_METADATA_KEY, token.toByteArray());

        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        assertEquals(authStub.getPrincipal(Empty.getDefaultInstance()).getValue(), "role-token");
    }

    @Test
    public void testAuthenticationRoleTokenInvalidSignature() throws Exception {
        AuthRoleTokenInfo role = AuthRoleTokenInfo.newBuilder()
                .setRole("role-token")
                .setExpires(System.currentTimeMillis() + 3600*1000)
                .build();

        AuthRoleToken token = AuthRoleToken.newBuilder()
                .setRoleInfo(role)
                .setSignature(ByteString.copyFrom(new byte[32]))
                .build();

        Metadata headers = new Metadata();
        headers.put(AUTH_ROLE_TOKEN_METADATA_KEY, token.toByteArray());

        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        try {
            authStub.getPrincipal(Empty.getDefaultInstance());
            fail("Should have thrown UNAUTHENTICATED exception");
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
        }
    }

    @Test
    public void testAuthenticationRoleTokenExpired() throws Exception {
        AuthRoleTokenInfo role = AuthRoleTokenInfo.newBuilder()
                .setRole("role-token")
                .setExpires(System.currentTimeMillis() - 3600*1000)
                .build();

        byte[] signature = signer.computeSignature(role.toByteArray());

        AuthRoleToken token = AuthRoleToken.newBuilder()
                .setRoleInfo(role)
                .setSignature(ByteString.copyFrom(signature))
                .build();

        Metadata headers = new Metadata();
        headers.put(AUTH_ROLE_TOKEN_METADATA_KEY, token.toByteArray());

        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        try {
            authStub.getPrincipal(Empty.getDefaultInstance());
            fail("Should have thrown UNAUTHENTICATED exception");
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
        }
    }

    @Test
    public void testAuthenticationSasl() throws Exception {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setSaslJaasClientAllowedIds(".*" + "client" + ".*");
        conf.setSaslJaasServerSectionName("PulsarBroker");

        AuthenticationProvider provider = new AuthenticationProviderSasl();
        provider.initialize(conf);
        doReturn(provider).when(authenticationService).getAuthenticationProvider(provider.getAuthMethodName());

        AuthenticationDataProvider dataProvider =  authSasl.getAuthData(localHostname);

        // Init
        AuthData initData1 = dataProvider.authenticate(AuthData.of(AuthData.INIT_AUTH_DATA));
        CommandAuth auth = CommandAuth.newBuilder()
                .setAuthMethod(provider.getAuthMethodName())
                .setAuthData(ByteString.copyFrom(initData1.getBytes()))
                .build();

        Metadata headers = new Metadata();
        headers.put(AUTH_METADATA_KEY, auth.toByteArray());
        AuthGrpc.AuthBlockingStub authStub = MetadataUtils.attachHeaders(this.stub, headers);

        org.apache.pulsar.protocols.grpc.api.AuthData challenge = null;
        try {
            authStub.getPrincipal(Empty.getDefaultInstance());
            fail("Should have received challenge");
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
            challenge = CommandAuthChallenge.parseFrom(e.getTrailers().get(AUTHCHALLENGE_METADATA_KEY)).getChallenge();
        }

        AuthData initData2 = dataProvider.authenticate(AuthData.of(challenge.getAuthData().toByteArray()));

        // Wrong session id
        CommandAuthResponse authResponse = CommandAuthResponse.newBuilder()
                .setResponse(getResponseForChallenge(challenge, initData2.getBytes())
                        .setAuthStateId(challenge.getAuthStateId() + 1))
                .build();
        headers = new Metadata();
        headers.put(AUTHRESPONSE_METADATA_KEY, authResponse.toByteArray());

        authStub = MetadataUtils.attachHeaders(this.stub, headers);

        try {
            authStub.getPrincipal(Empty.getDefaultInstance());
            fail("Should have thrown unauthenticated exception");
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
            assertNull(e.getTrailers().get(AUTH_ROLE_TOKEN_METADATA_KEY));
        }

        // Wrong method
        authResponse = CommandAuthResponse.newBuilder()
                .setResponse(getResponseForChallenge(challenge, initData2.getBytes())
                        .setAuthMethodName("none"))
                .build();
        headers = new Metadata();
        headers.put(AUTHRESPONSE_METADATA_KEY, authResponse.toByteArray());

        authStub = MetadataUtils.attachHeaders(this.stub, headers);

        try {
            authStub.getPrincipal(Empty.getDefaultInstance());
            fail("Should have thrown unauthenticated exception");
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
            assertNull(e.getTrailers().get(AUTH_ROLE_TOKEN_METADATA_KEY));
        }

        // Incorrect auth data
        authResponse = CommandAuthResponse.newBuilder()
                .setResponse(getResponseForChallenge(challenge, initData1.getBytes()))
                .build();
        headers = new Metadata();
        headers.put(AUTHRESPONSE_METADATA_KEY, authResponse.toByteArray());

        authStub = MetadataUtils.attachHeaders(this.stub, headers);

        try {
            authStub.getPrincipal(Empty.getDefaultInstance());
            fail("Should have thrown unauthenticated exception");
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
            assertNull(e.getTrailers().get(AUTH_ROLE_TOKEN_METADATA_KEY));
        }

        // Good response
        authResponse = CommandAuthResponse.newBuilder()
                .setResponse(getResponseForChallenge(challenge, initData2.getBytes()))
                .build();
        headers = new Metadata();
        headers.put(AUTHRESPONSE_METADATA_KEY, authResponse.toByteArray());

        authStub = MetadataUtils.attachHeaders(this.stub, headers);
        try {
            authStub.getPrincipal(Empty.getDefaultInstance());
            fail("Should have received auth token");
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
            AuthRoleToken authToken = AuthRoleToken.parseFrom(e.getTrailers().get(AUTH_ROLE_TOKEN_METADATA_KEY));
            assertEquals(authToken.getRoleInfo().getRole(), "client@EXAMPLE.COM");
            assertTrue(authToken.getRoleInfo().getExpires() > System.currentTimeMillis());
        }
    }

    private org.apache.pulsar.protocols.grpc.api.AuthData.Builder getResponseForChallenge(org.apache.pulsar.protocols.grpc.api.AuthData challenge, byte[] authData) throws AuthenticationException {
        return org.apache.pulsar.protocols.grpc.api.AuthData.newBuilder()
                .setAuthMethodName(challenge.getAuthMethodName())
                .setAuthStateId(challenge.getAuthStateId())
                .setAuthData(ByteString.copyFrom(authData));
    }

    private static class AuthGrpcService extends AuthGrpc.AuthImplBase {

        @Override
        public void getPrincipal(Empty request, StreamObserver<StringValue> responseObserver) {
            String role = AUTH_ROLE_CTX_KEY.get();
            StringValue value = StringValue.newBuilder().setValue(role != null ? role : "unauthenticated user").build();
            responseObserver.onNext(value);
            responseObserver.onCompleted();
        }
    }

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

    @AfterClass
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

}
