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

public class TimeAverageMessageDataTest {
    @Test
    public void testCompareToContract() {
        Random rnd = new Random();
        List<TimeAverageMessageData> list = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            TimeAverageMessageData data = new TimeAverageMessageData(1);
            double msgThroughputIn = 4 * 75000 * rnd.nextDouble();
            double msgThroughputOut = 75000000 - (4 * (75000 * rnd.nextDouble()));
            double msgRateIn = 4 * 75 * rnd.nextDouble();
            double msgRateOut = 75000 - (4 * 75 * rnd.nextDouble());
            data.update(msgThroughputIn, msgThroughputOut, msgRateIn, msgRateOut);
            list.add(data);
        }
        // this would throw "java.lang.IllegalArgumentException: Comparison method violates its general contract!"
        // if compareTo() is not implemented correctly.
        list.sort(TimeAverageMessageData::compareTo);
    }
}