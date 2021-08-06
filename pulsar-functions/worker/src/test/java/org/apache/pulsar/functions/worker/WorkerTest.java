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

import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WorkerTest {

    @Test
    public void testWorkerFailsForMismatchedClusterNames() {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsCluster("pulsar");
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setClusterName("cluster");
        Assert.assertThrows(IllegalArgumentException.class, () -> new Worker(workerConfig, serviceConfiguration));
    }

    @Test
    public void testWorkerClassInitializesWithValidClusterName() {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setPulsarFunctionsCluster("pulsar");
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        serviceConfiguration.setClusterName("pulsar");
        // Test only verifies that initialization does not throw an exception
        // Note that calling the start() method would fail because the configuration is incomplete
        new Worker(workerConfig, serviceConfiguration);
    }

}
