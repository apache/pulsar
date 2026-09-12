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
package org.apache.pulsar.policies.data.loadbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.testng.annotations.Test;

public class NamespaceBundleStatsTest {

    @Test
    public void testCompareToContract() {
        Random rnd = new Random();
        List<NamespaceBundleStats> stats = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            NamespaceBundleStats s = new NamespaceBundleStats();
            s.msgThroughputIn = 4 * 75000 * rnd.nextDouble();
            s.msgThroughputOut = 75000000 - (4 * (75000 * rnd.nextDouble()));
            s.msgRateIn = 4 * 75 * rnd.nextDouble();
            s.msgRateOut = 75000 - (4 * 75 * rnd.nextDouble());
            s.topics = i;
            s.consumerCount = i;
            s.producerCount = 4 * rnd.nextInt(375);
            s.cacheSize = 75000000 - (rnd.nextInt(4 * 75000));
            stats.add(s);
        }
        // this would throw "java.lang.IllegalArgumentException: Comparison method violates its general contract!"
        // if compareTo() is not implemented correctly.
        stats.sort(NamespaceBundleStats::compareTo);
    }
}