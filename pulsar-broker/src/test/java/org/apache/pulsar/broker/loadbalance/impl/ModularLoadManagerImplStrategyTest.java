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

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

/**
 * Tests the pairing of the placement strategy and the load shedding strategy in
 * {@link ModularLoadManagerImpl#createLoadBalanceStrategies(ServiceConfiguration)}.
 */
@Test(groups = "broker")
public class ModularLoadManagerImplStrategyTest {

    @Test
    public void testDefaultsPairAvgShedderForSheddingAndPlacement() {
        ServiceConfiguration conf = new ServiceConfiguration();

        assertThat(conf.getLoadBalancerLoadSheddingStrategy()).isEqualTo(AvgShedder.class.getName());
        assertThat(conf.getLoadBalancerLoadPlacementStrategy()).isEqualTo(AvgShedder.class.getName());
        assertThat(conf.getMaxUnloadPercentage()).isEqualTo(0.5);
        assertThat(conf.isLoadBalancerDistributeBundlesEvenlyEnabled()).isFalse();

        ModularLoadManagerImpl.LoadBalanceStrategies strategies =
                ModularLoadManagerImpl.createLoadBalanceStrategies(conf);

        assertThat(strategies.placementStrategy()).isInstanceOf(AvgShedder.class);
        // the shedding and the placement strategy must be the same instance so that the shedder's planned
        // destinations are honored by placement
        assertThat(strategies.loadSheddingStrategy()).isSameAs(strategies.placementStrategy());
    }

    @Test
    public void testExplicitClassicShedderFallsBackToLeastLongTermMessageRatePlacement() {
        // a configuration written before AvgShedder became the default typically sets only the shedding strategy
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setLoadBalancerLoadSheddingStrategy(ThresholdShedder.class.getName());

        ModularLoadManagerImpl.LoadBalanceStrategies strategies =
                ModularLoadManagerImpl.createLoadBalanceStrategies(conf);

        assertThat(strategies.loadSheddingStrategy()).isInstanceOf(ThresholdShedder.class);
        assertThat(strategies.placementStrategy()).isInstanceOf(LeastLongTermMessageRate.class);
    }

    @Test
    public void testExplicitPlacementIsKeptWithClassicShedder() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setLoadBalancerLoadSheddingStrategy(ThresholdShedder.class.getName());
        conf.setLoadBalancerLoadPlacementStrategy(LeastResourceUsageWithWeight.class.getName());

        ModularLoadManagerImpl.LoadBalanceStrategies strategies =
                ModularLoadManagerImpl.createLoadBalanceStrategies(conf);

        assertThat(strategies.loadSheddingStrategy()).isInstanceOf(ThresholdShedder.class);
        assertThat(strategies.placementStrategy()).isInstanceOf(LeastResourceUsageWithWeight.class);
    }

    @Test
    public void testAvgShedderSheddingWithOtherPlacementIsNotPaired() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setLoadBalancerLoadPlacementStrategy(LeastResourceUsageWithWeight.class.getName());

        ModularLoadManagerImpl.LoadBalanceStrategies strategies =
                ModularLoadManagerImpl.createLoadBalanceStrategies(conf);

        assertThat(strategies.loadSheddingStrategy()).isInstanceOf(AvgShedder.class);
        assertThat(strategies.placementStrategy()).isInstanceOf(LeastResourceUsageWithWeight.class);
    }
}
