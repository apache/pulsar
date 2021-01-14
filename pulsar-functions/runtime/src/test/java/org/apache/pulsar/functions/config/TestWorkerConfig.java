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
package org.apache.pulsar.functions.config;

import org.apache.pulsar.functions.worker.WorkerConfig;
import org.junit.Assert;
import org.testng.annotations.Test;

public class TestWorkerConfig {
    @Test
    public void validateAuthenticationCompatibleWorkerConfig() {
        WorkerConfig workerConfig = new WorkerConfig();

        workerConfig.setAuthenticationEnabled(false);
        Assert.assertFalse(workerConfig.isAuthenticationEnabled());
        workerConfig.setBrokerClientAuthenticationEnabled(null);
        Assert.assertFalse(workerConfig.isBrokerClientAuthenticationEnabled());

        workerConfig.setAuthenticationEnabled(true);
        Assert.assertTrue(workerConfig.isAuthenticationEnabled());
        workerConfig.setBrokerClientAuthenticationEnabled(null);
        Assert.assertTrue(workerConfig.isBrokerClientAuthenticationEnabled());

        workerConfig.setBrokerClientAuthenticationEnabled(true);
        workerConfig.setAuthenticationEnabled(false);
        Assert.assertTrue(workerConfig.isBrokerClientAuthenticationEnabled());
        workerConfig.setAuthenticationEnabled(true);
        Assert.assertTrue(workerConfig.isBrokerClientAuthenticationEnabled());

        workerConfig.setBrokerClientAuthenticationEnabled(false);
        workerConfig.setAuthenticationEnabled(false);
        Assert.assertFalse(workerConfig.isBrokerClientAuthenticationEnabled());
        workerConfig.setAuthenticationEnabled(true);
        Assert.assertFalse(workerConfig.isBrokerClientAuthenticationEnabled());
    }
}
