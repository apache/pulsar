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
package org.apache.pulsar.broker.loadbalance.impl;

import lombok.Cleanup;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class LinuxBrokerHostUsageImplTest {

    @Test
    public void checkOverrideBrokerNicSpeedGbps() {
        @Cleanup("shutdown")
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        LinuxBrokerHostUsageImpl linuxBrokerHostUsage =
                new LinuxBrokerHostUsageImpl(1, Optional.of(3.0), executorService);
        List<String> nics = new ArrayList<>();
        nics.add("1");
        nics.add("2");
        nics.add("3");
        double totalLimit = linuxBrokerHostUsage.getTotalNicLimitWithConfiguration(nics);
        Assert.assertEquals(totalLimit, 3.0 * 1000 * 1000 * 3);
    }
}
