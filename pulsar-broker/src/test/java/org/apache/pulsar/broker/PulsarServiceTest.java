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

import static org.mockito.Mockito.mock;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.apache.pulsar.functions.worker.WorkerService;
import org.testng.annotations.Test;


@Slf4j
public class PulsarServiceTest {

    @Test
    public void testGetWorkerService() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setZookeeperServers("localhost");
        configuration.setClusterName("clusterName");
        configuration.setFunctionsWorkerEnabled(true);
        PulsarService pulsarService = new PulsarService(configuration, new WorkerConfig(),
                Optional.of(mock(WorkerService.class)), (exitCode) -> {});

        pulsarService.getWorkerService();
    }

    /**
     * Verifies that the getWorkerService throws {@link UnsupportedOperationException}
     * when functionsWorkerEnabled is set to false .
     */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetWorkerServiceException() throws Exception {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setZookeeperServers("localhost");
        configuration.setClusterName("clusterName");
        configuration.setFunctionsWorkerEnabled(false);
        PulsarService pulsarService = new PulsarService(configuration, new WorkerConfig(),
                Optional.empty(), (exitCode) -> {});

        pulsarService.getWorkerService();
    }
}
