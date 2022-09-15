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
package org.apache.pulsar.broker;

import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.LoadSheddingTask;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class PulsarServiceCloseTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected PulsarService startBrokerWithoutAuthorization(ServiceConfiguration conf) throws Exception {
        conf.setBrokerShutdownTimeoutMs(1000 * 60 * 5);
        conf.setLoadBalancerSheddingIntervalMinutes(30);
        PulsarService pulsar = spy(newPulsarService(conf));
        setupBrokerMocks(pulsar);
        beforePulsarStartMocks(pulsar);
        pulsar.start();
        log.info("Pulsar started. brokerServiceUrl: {} webServiceAddress: {}", pulsar.getBrokerServiceUrl(),
                pulsar.getWebServiceAddress());
        return pulsar;
    }

    @Test(timeOut = 30_000)
    public void closeInTimeTest() throws Exception {
        LoadSheddingTask task = pulsar.getLoadSheddingTask();
        boolean isCancel = WhiteboxImpl.getInternalState(task, "isCancel");
        assertFalse(isCancel);
        ScheduledFuture<?> loadSheddingFuture = WhiteboxImpl.getInternalState(task, "future");
        assertFalse(loadSheddingFuture.isCancelled());

        // The pulsar service is not used, so it should be closed gracefully in short time.
        pulsar.close();

        isCancel = WhiteboxImpl.getInternalState(task, "isCancel");
        assertTrue(isCancel);
        loadSheddingFuture = WhiteboxImpl.getInternalState(task, "future");
        assertTrue(loadSheddingFuture.isCancelled());
    }

}
