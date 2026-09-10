/*
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
package org.apache.pulsar.broker;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.CA_CERT_FILE_PATH;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.getTlsFileForClient;
import static org.apache.pulsar.client.impl.SameAuthParamsLookupAutoClusterFailover.PulsarServiceState;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.NetworkErrorTestBase;
import org.apache.pulsar.broker.service.OneWayReplicatorTestBase;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.SameAuthParamsLookupAutoClusterFailover;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class SameAuthParamsLookupAutoClusterFailoverTest extends OneWayReplicatorTestBase {

    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterMethod(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @DataProvider(name = "enabledTls")
    public Object[][] enabledTls () {
        return new Object[][] {
            {true},
            {false}
        };
    }
    @SuppressWarnings("deprecation")

    // Each state-convergence phase below waits up to 3 minutes. The probe timeout is 3s
    // and recoverThreshold=5, so a transient probe failure during recovery resets the
    // counter and a phase can need ~30s of healthy probes to recover. With 3 phases plus
    // cluster startup/teardown, allow 12 minutes overall to absorb slow CI agents.
    @Test(dataProvider = "enabledTls", timeOut = 720 * 1000)
    public void testAutoClusterFailover(boolean enabledTls) throws Exception {
        // Start clusters.
        setup();
        ServerSocket dummyServer = new ServerSocket(NetworkErrorTestBase.getOneFreePort());

        // Initialize client.
        String urlProxy = enabledTls ? "pulsar+tls://127.0.0.1:" + dummyServer.getLocalPort()
                : "pulsar://127.0.0.1:" + dummyServer.getLocalPort();
        String url1 = enabledTls ? pulsar1.getBrokerServiceUrlTls() : pulsar1.getBrokerServiceUrl();
        String url2 = enabledTls ? pulsar2.getBrokerServiceUrlTls() : pulsar2.getBrokerServiceUrl();
        final String[] urlArray = new String[]{url1, urlProxy, url2};
        final SameAuthParamsLookupAutoClusterFailover failover = SameAuthParamsLookupAutoClusterFailover.builder()
                .pulsarServiceUrlArray(urlArray)
                .failoverThreshold(5)
                .recoverThreshold(5)
                .checkHealthyIntervalMs(100)
                .testTopic("a/b/c")
                .markTopicNotFoundAsAvailable(true)
                .build();
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrlProvider(failover);
        if (enabledTls) {
            Map<String, String> authParams = new HashMap<>();
            authParams.put("tlsCertFile", getTlsFileForClient("admin.cert"));
            authParams.put("tlsKeyFile", getTlsFileForClient("admin.key-pk8"));
            clientBuilder.authentication(AuthenticationTls.class.getName(), authParams)
                .enableTls(true)
                .allowTlsInsecureConnection(false)
                .tlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        }
        final PulsarClient client = clientBuilder.build();
        final ScheduledExecutorService executor = WhiteboxImpl.getInternalState(failover, "executor");
        final PulsarServiceState[] stateArray =
                WhiteboxImpl.getInternalState(failover, "pulsarServiceStateArray");

        // Test all things is fine.
        final String tp = BrokerTestUtil.newUniqueName(nonReplicatedNamespace + "/tp");
        final Producer<String> producer = client.newProducer(Schema.STRING).topic(tp).create();
        producer.send("0");
        Assert.assertEquals(failover.getCurrentPulsarServiceIndex(), 0);

        assertStatesEqual(executor, stateArray,
                PulsarServiceState.Healthy, PulsarServiceState.Healthy, PulsarServiceState.Healthy);

        // Test failover 0 --> 2.
        pulsar1.close();
        awaitStatesAndIndex(executor, stateArray, failover, 2,
                PulsarServiceState.Failed, PulsarServiceState.Failed, PulsarServiceState.Healthy);
        producer.send("0->2");

        // Test recover 2 --> 1.
        executor.execute(() -> {
            urlArray[1] = url2;
        });
        awaitStatesAndIndex(executor, stateArray, failover, 1,
                PulsarServiceState.Failed, PulsarServiceState.Healthy, PulsarServiceState.Healthy);
        producer.send("2->1");

        // Test recover 1 --> 0.
        executor.execute(() -> {
            urlArray[0] = url2;
        });
        awaitStatesAndIndex(executor, stateArray, failover, 0,
                PulsarServiceState.Healthy, PulsarServiceState.Healthy, PulsarServiceState.Healthy);
        producer.send("1->0");

        // cleanup.
        producer.close();
        client.close();
        dummyServer.close();
    }

    @Test
    public void testInitializeCanOnlyBeCalledOnce() throws Exception {
        setup();
        final SameAuthParamsLookupAutoClusterFailover failover = SameAuthParamsLookupAutoClusterFailover.builder()
                .pulsarServiceUrlArray(new String[]{pulsar1.getBrokerServiceUrl()})
                .checkHealthyIntervalMs(1000)
                .build();

        try (PulsarClient client = PulsarClient.builder().serviceUrlProvider(failover).build()) {
            Throwable error = Assert.expectThrows(IllegalStateException.class, () -> failover.initialize(client));
            Assert.assertEquals(error.getMessage(), "ServiceUrlProvider has already been initialized");
        }
    }

    /**
     * Wait for the state machine to converge to the expected per-index states and current index.
     * The state read happens on the failover executor to avoid races with the periodic check task,
     * and producer/lookup operations are kept out of the polling loop so a slow message send does
     * not consume the convergence budget.
     */
    private static void awaitStatesAndIndex(ScheduledExecutorService executor, PulsarServiceState[] stateArray,
                                            SameAuthParamsLookupAutoClusterFailover failover,
                                            int expectedIndex,
                                            PulsarServiceState... expectedStates) {
        Awaitility.await().atMost(180, TimeUnit.SECONDS).untilAsserted(() -> {
            assertStatesEqual(executor, stateArray, expectedStates);
            Assert.assertEquals(failover.getCurrentPulsarServiceIndex(), expectedIndex);
        });
    }

    private static void assertStatesEqual(ScheduledExecutorService executor, PulsarServiceState[] stateArray,
                                          PulsarServiceState... expected) throws Exception {
        CompletableFuture<PulsarServiceState[]> snapshot = new CompletableFuture<>();
        executor.submit(() -> snapshot.complete(stateArray.clone()));
        PulsarServiceState[] actual = snapshot.get(10, TimeUnit.SECONDS);
        Assert.assertEquals(actual, expected);
    }

    @Override
    protected void cleanupPulsarResources() {
        // Nothing to do.
    }

}
