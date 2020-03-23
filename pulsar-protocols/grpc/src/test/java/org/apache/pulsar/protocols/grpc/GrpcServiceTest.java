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

import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import org.apache.pulsar.broker.NoOpShutdownService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.protocols.grpc.api.CommandConnect;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopic;
import org.apache.pulsar.protocols.grpc.api.CommandLookupTopicResponse;
import org.apache.pulsar.protocols.grpc.api.PulsarGrpc;
import org.apache.pulsar.zookeeper.MockedZooKeeperClientFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor;
import static org.apache.pulsar.protocols.grpc.Constants.AUTH_METADATA_KEY;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class GrpcServiceTest {

    private PulsarService pulsar;
    private GrpcService grpcService = new GrpcService();
    private static final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private static final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
    private static final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/certificate/client.crt";
    private static final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/certificate/client.key";

    /**
     * Test that if enableTls option is enabled, GrpcService is available both on plaintext and TLS ports.
     *
     * @throws Exception
     */
    @Test
    public void testTlsEnabled() throws Exception {
        setupEnv(true, false, false);

        // Make requests both HTTP and HTTPS. The requests should succeed
        try {
            makeGrpcRequest(false, false);
        } catch (Exception e) {
            Assert.fail("HTTP request shouldn't fail ", e);
        }
        try {
            makeGrpcRequest(true, false);
        } catch (Exception e) {
            Assert.fail("HTTPS request shouldn't fail ", e);
        }
    }

    /**
     * Test that if enableTls option is disabled, GrpcService is available only on plaintext port.
     *
     * @throws Exception
     */
    @Test
    public void testTlsDisabled() throws Exception {
        setupEnv(false, false, false);

        // Make requests both HTTP and HTTPS. Only the HTTP request should succeed
        try {
            makeGrpcRequest(false, false);
        } catch (Exception e) {
            Assert.fail("HTTP request shouldn't fail ", e);
        }
        try {
            makeGrpcRequest(true, false);
            Assert.fail("HTTPS request should fail ");
        } catch (StatusRuntimeException e) {
            // Expected
        }
    }

    /**
     * Test that if enableAuth option and allowInsecure option are enabled, GrpcService requires trusted/untrusted client
     * certificate.
     *
     * @throws Exception
     */
    @Test
    public void testTlsAuthAllowInsecure() throws Exception {
        setupEnv(true, true, true);

        // Only the request with client certificate should succeed
        try {
            makeGrpcRequest(true, false);
            Assert.fail("Request without client certificate should fail");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
        }
        try {
            makeGrpcRequest(true, true);
        } catch (Exception e) {
            Assert.fail("Request with client certificate shouldn't fail", e);
        }
    }

    /**
     * Test that if enableAuth option is enabled, GrpcService requires trusted client certificate.
     *
     * @throws Exception
     */
    @Test
    public void testTlsAuthDisallowInsecure() throws Exception {
        setupEnv(true, true, false);

        // Only the request with trusted client certificate should succeed
        try {
            makeGrpcRequest(true, false);
            Assert.fail("Request without client certificate should fail");
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().getCode(), Status.Code.UNAUTHENTICATED);
        }
        try {
            makeGrpcRequest(true, true);
        } catch (Exception e) {
            Assert.fail("Request with client certificate shouldn't fail", e);
        }
    }

    private String makeGrpcRequest(boolean useTls, boolean useAuth) throws Exception {
        NettyChannelBuilder channelBuilder;
        if (useTls) {
            SslContext sslContext;
            if (useAuth) {
                sslContext = SecurityUtility.createNettySslContextForClient(true, null,
                        TLS_CLIENT_CERT_FILE_PATH, TLS_CLIENT_KEY_FILE_PATH);
            } else {
                sslContext = SecurityUtility.createNettySslContextForClient(true, null);
            }
            channelBuilder = NettyChannelBuilder
                    .forAddress("localhost", grpcService.getListenPortTLS().orElse(-1))
                    .sslContext(sslContext);
            if (useAuth) {
                Metadata authHeaders = new Metadata();
                authHeaders.put(AUTH_METADATA_KEY, CommandConnect.newBuilder().setAuthMethodName("tls").build().toByteArray());
                channelBuilder.intercept(newAttachHeadersInterceptor(authHeaders));
            }
        } else {
            channelBuilder = NettyChannelBuilder
                    .forAddress("localhost", grpcService.getListenPort().orElse(-1))
                    .negotiationType(NegotiationType.PLAINTEXT);
        }
        ManagedChannel channel = channelBuilder.build();
        PulsarGrpc.PulsarBlockingStub stub = PulsarGrpc.newBlockingStub(channel);
        String result;
        try {
            CommandLookupTopicResponse response = stub.lookupTopic(CommandLookupTopic.newBuilder().setTopic("persistent://my-property/local/my-namespace/my-topic").build());
            result = response.getGrpcServiceHost();
        } finally {
            channel.shutdown();
            channel.awaitTermination(30, TimeUnit.SECONDS);
        }
        log.info("Response: {}", result);
        return result;
    }

    MockedZooKeeperClientFactoryImpl zkFactory = new MockedZooKeeperClientFactoryImpl();

    private void setupEnv(boolean enableTls, boolean enableAuth, boolean allowInsecure) throws Exception {
        Set<String> providers = new HashSet<>();
        providers.add("org.apache.pulsar.broker.authentication.AuthenticationProviderTls");

        Set<String> roles = new HashSet<>();
        roles.add("client");

        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.getProperties().setProperty("grpcServicePort", "0");
        if (enableTls) {
            config.setWebServicePortTls(Optional.of(0));
            config.getProperties().setProperty("grpcServicePortTls", "0");
        }
        config.setAuthenticationEnabled(enableAuth);
        config.setAuthenticationProviders(providers);
        config.setAuthorizationEnabled(false);
        config.setSuperUserRoles(roles);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsAllowInsecureConnection(allowInsecure);
        config.setTlsTrustCertsFilePath(allowInsecure ? "" : TLS_CLIENT_CERT_FILE_PATH);
        config.setClusterName("local");
        config.setAdvertisedAddress("localhost"); // TLS certificate expects localhost
        config.setZookeeperServers("localhost:2181");
        pulsar = spy(new PulsarService(config));
        pulsar.setShutdownService(new NoOpShutdownService());
        doReturn(zkFactory).when(pulsar).getZooKeeperClientFactory();
        doReturn(new MockedBookKeeperClientFactory()).when(pulsar).newBookKeeperClientFactory();
        Map<String, String> protocolDataToAdvertise = new HashMap<>();
        doReturn(protocolDataToAdvertise).when(pulsar).getProtocolDataToAdvertise();
        pulsar.start();

        String BROKER_URL_BASE = "http://localhost:" + pulsar.getListenPortHTTP().get();
        String BROKER_URL_BASE_TLS = "https://localhost:" + pulsar.getListenPortHTTPS().orElse(-1);
        String serviceUrl = BROKER_URL_BASE;

        PulsarAdminBuilder adminBuilder = PulsarAdmin.builder();
        if (enableTls && enableAuth) {
            serviceUrl = BROKER_URL_BASE_TLS;

            Map<String, String> authParams = new HashMap<>();
            authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
            authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);

            adminBuilder.authentication(AuthenticationTls.class.getName(), authParams).allowTlsInsecureConnection(true);
        }

        PulsarAdmin pulsarAdmin = adminBuilder.serviceHttpUrl(serviceUrl).build();

        try {
            pulsarAdmin.clusters().createCluster(config.getClusterName(),
                    new ClusterData(pulsar.getSafeWebServiceAddress()));
        } catch (PulsarAdminException.ConflictException ce) {
            // This is OK.
        } finally {
            pulsarAdmin.close();
        }

        grpcService = new GrpcService();
        grpcService.initialize(config);
        grpcService.start(pulsar.getBrokerService());
        protocolDataToAdvertise.put(grpcService.protocolName(), grpcService.getProtocolDataToAdvertise());
        pulsar.getLoadManager().get().writeLoadReportOnZookeeper();
        pulsar.getLoadManager().get().stop();
        pulsar.getLoadManager().get().start();
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        try {
            grpcService.close();
            pulsar.close();
        } catch (Exception e) {
            Assert.fail("Got exception while closing the pulsar instance ", e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(GrpcServiceTest.class);
}
