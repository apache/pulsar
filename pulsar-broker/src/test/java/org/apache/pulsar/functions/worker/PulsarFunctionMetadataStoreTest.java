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
package org.apache.pulsar.functions.worker;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.functions.instance.state.PulsarMetadataStateStoreProviderImpl;
import org.testng.annotations.Test;

/**
 * Test Pulsar sink on function
 */
@Slf4j
@Test
public class PulsarFunctionMetadataStoreTest extends PulsarFunctionLocalRunTest {


    protected WorkerConfig createWorkerConfig(ServiceConfiguration config) {
        WorkerConfig wc = super.createWorkerConfig(config);
        wc.setStateStorageProviderImplementation(PulsarMetadataStateStoreProviderImpl.class.getName());
        wc.setStateStorageServiceUrl("memory:local");
        return wc;
    }

    @Test
    public void testE2EPulsarFunctionLocalRun() throws Throwable {
        runWithPulsarFunctionsClassLoader(() -> testE2EPulsarFunctionLocalRun(null));
    }
}
