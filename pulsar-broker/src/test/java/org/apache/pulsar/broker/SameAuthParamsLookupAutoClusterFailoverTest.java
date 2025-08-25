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
import io.netty.channel.EventLoopGroup;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

    @Test(dataProvider = "enabledTls", timeOut = 240 * 1000)
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
                .checkHealthyIntervalMs(300)
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
        failover.initialize(client);
        final EventLoopGroup executor = WhiteboxImpl.getInternalState(failover, "executor");
        final PulsarServiceState[] stateArray =
                WhiteboxImpl.getInternalState(failover, "pulsarServiceStateArray");

        // Test all things is fine.
        final String tp = BrokerTestUtil.newUniqueName(nonReplicatedNamespace + "/tp");
        final Producer<String> producer = client.newProducer(Schema.STRING).topic(tp).create();
        producer.send("0");
        Assert.assertEquals(failover.getCurrentPulsarServiceIndex(), 0);

        CompletableFuture<Boolean> checkStatesFuture1 = new CompletableFuture<>();
        executor.submit(() -> {
            boolean res = stateArray[0] == PulsarServiceState.Healthy;
            res = res & stateArray[1] == PulsarServiceState.Healthy;
            res = res & stateArray[2] == PulsarServiceState.Healthy;
            checkStatesFuture1.complete(res);
        });
        Assert.assertTrue(checkStatesFuture1.join());

        // Test failover 0 --> 2.
        pulsar1.close();
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            CompletableFuture<Boolean> checkStatesFuture2 = new CompletableFuture<>();
            executor.submit(() -> {
                boolean res = stateArray[0] == PulsarServiceState.Failed;
                res = res & stateArray[1] == PulsarServiceState.Failed;
                res = res & stateArray[2] == PulsarServiceState.Healthy;
                checkStatesFuture2.complete(res);
            });
            Assert.assertTrue(checkStatesFuture2.join());
            producer.send("0->2");
            Assert.assertEquals(failover.getCurrentPulsarServiceIndex(), 2);
        });

        // Test recover 2 --> 1.
        executor.execute(() -> {
            urlArray[1] = url2;
        });
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            CompletableFuture<Boolean> checkStatesFuture3 = new CompletableFuture<>();
            executor.submit(() -> {
                boolean res = stateArray[0] == PulsarServiceState.Failed;
                res = res & stateArray[1] == PulsarServiceState.Healthy;
                res = res & stateArray[2] == PulsarServiceState.Healthy;
                checkStatesFuture3.complete(res);
            });
            Assert.assertTrue(checkStatesFuture3.join());
            producer.send("2->1");
            Assert.assertEquals(failover.getCurrentPulsarServiceIndex(), 1);
        });

        // Test recover 1 --> 0.
        executor.execute(() -> {
            urlArray[0] = url2;
        });
        Awaitility.await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            CompletableFuture<Boolean> checkStatesFuture4 = new CompletableFuture<>();
            executor.submit(() -> {
                boolean res = stateArray[0] == PulsarServiceState.Healthy;
                res = res & stateArray[1] == PulsarServiceState.Healthy;
                res = res & stateArray[2] == PulsarServiceState.Healthy;
                checkStatesFuture4.complete(res);
            });
            Assert.assertTrue(checkStatesFuture4.join());
            producer.send("1->0");
            Assert.assertEquals(failover.getCurrentPulsarServiceIndex(), 0);
        });

        // cleanup.
        producer.close();
        client.close();
        dummyServer.close();
    }

    @Override
    protected void cleanupPulsarResources() {
        // Nothing to do.
    }

}
