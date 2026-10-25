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
package org.apache.pulsar.functions.worker;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;

/**
 * Runs {@link PulsarFunctionTlsTest} against brokers that also expose plaintext listeners, with
 * {@code brokerClientTlsEnabled=true} and the deprecated worker {@code useTls} left unset.
 */
@Test(groups = "functions-worker")
public class PulsarFunctionTlsMixedListenersTest extends PulsarFunctionTlsTest {

    @Override
    protected boolean enablePlaintextListeners() {
        return true;
    }

    @Test
    public void testWorkerUsesTlsUrls() {
        for (int i = 0; i < BROKER_COUNT; i++) {
            WorkerConfig workerConfig = fnWorkerServices[i].getWorkerConfig();
            assertEquals(workerConfig.getPulsarServiceUrl(), pulsarServices[i].getBrokerServiceUrlTls());
            assertEquals(workerConfig.getPulsarWebServiceUrl(), pulsarServices[i].getWebServiceAddressTls());
            assertEquals(workerConfig.getFunctionWebServiceUrl(), pulsarServices[i].getWebServiceAddressTls());
        }
    }
}
