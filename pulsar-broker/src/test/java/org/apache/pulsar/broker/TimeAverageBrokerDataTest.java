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

import static org.testng.Assert.*;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class TimeAverageBrokerDataTest {

    @Test
    public void testIllegalArg() {
        TimeAverageBrokerData timeAverageBrokerData = new TimeAverageBrokerData();
        assertThrows(NullPointerException.class, () -> timeAverageBrokerData.reset(null, null, null));
    }

    @Test
    public void testResetMethodOfOneBundlesEmptyBundleMap() {
        TimeAverageBrokerData timeAverageBrokerData = new TimeAverageBrokerData();
        Set<String> bundles = new HashSet<>();
        bundles.add("a");
        Map<String, BundleData> emptyMap = new HashMap<>();
        NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.msgThroughputIn = 1;
        namespaceBundleStats.msgThroughputOut = 2;
        namespaceBundleStats.msgRateIn = 3;
        namespaceBundleStats.msgRateOut = 4;
        timeAverageBrokerData.reset(bundles, emptyMap, namespaceBundleStats);
        assertEquals(timeAverageBrokerData.getShortTermMsgThroughputIn(), 1);
        assertEquals(timeAverageBrokerData.getShortTermMsgThroughputOut(), 2);
        assertEquals(timeAverageBrokerData.getShortTermMsgRateIn(), 3);
        assertEquals(timeAverageBrokerData.getShortTermMsgRateOut(), 4);
        assertEquals(timeAverageBrokerData.getLongTermMsgThroughputIn(), 1);
        assertEquals(timeAverageBrokerData.getLongTermMsgThroughputOut(), 2);
        assertEquals(timeAverageBrokerData.getLongTermMsgRateIn(), 3);
        assertEquals(timeAverageBrokerData.getLongTermMsgRateOut(), 4);
    }

    @Test
    public void testResetMethodOfMultipleBundlesEmptyBundleMap() {
        TimeAverageBrokerData timeAverageBrokerData = new TimeAverageBrokerData();
        Set<String> bundles = new HashSet<>();
        bundles.add("a");
        bundles.add("b");
        Map<String, BundleData> emptyMap = new HashMap<>();
        NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.msgThroughputIn = 1;
        namespaceBundleStats.msgThroughputOut = 2;
        namespaceBundleStats.msgRateIn = 3;
        namespaceBundleStats.msgRateOut = 4;
        timeAverageBrokerData.reset(bundles, emptyMap, namespaceBundleStats);
        assertEquals(timeAverageBrokerData.getShortTermMsgThroughputIn(), 2);
        assertEquals(timeAverageBrokerData.getShortTermMsgThroughputOut(), 4);
        assertEquals(timeAverageBrokerData.getShortTermMsgRateIn(), 6);
        assertEquals(timeAverageBrokerData.getShortTermMsgRateOut(), 8);
        assertEquals(timeAverageBrokerData.getLongTermMsgThroughputIn(), 2);
        assertEquals(timeAverageBrokerData.getLongTermMsgThroughputOut(), 4);
        assertEquals(timeAverageBrokerData.getLongTermMsgRateIn(), 6);
        assertEquals(timeAverageBrokerData.getLongTermMsgRateOut(), 8);
    }

    @Test
    public void testResetMethodOfOneBundles() {
        TimeAverageBrokerData timeAverageBrokerData = new TimeAverageBrokerData();
        Set<String> bundles = new HashSet<>();
        bundles.add("a");
        Map<String, BundleData> data = new HashMap<>();
        TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
        timeAverageMessageData.setMsgThroughputIn(1);
        timeAverageMessageData.setMsgThroughputOut(2);
        timeAverageMessageData.setMsgRateIn(3);
        timeAverageMessageData.setMsgRateOut(4);
        BundleData bundleData = new BundleData();
        bundleData.setLongTermData(timeAverageMessageData);
        bundleData.setShortTermData(timeAverageMessageData);
        data.put("a", bundleData);
        timeAverageBrokerData.reset(bundles, data, null);
        assertEquals(timeAverageBrokerData.getShortTermMsgThroughputIn(), 1);
        assertEquals(timeAverageBrokerData.getShortTermMsgThroughputOut(), 2);
        assertEquals(timeAverageBrokerData.getShortTermMsgRateIn(), 3);
        assertEquals(timeAverageBrokerData.getShortTermMsgRateOut(), 4);
        assertEquals(timeAverageBrokerData.getLongTermMsgThroughputIn(), 1);
        assertEquals(timeAverageBrokerData.getLongTermMsgThroughputOut(), 2);
        assertEquals(timeAverageBrokerData.getLongTermMsgRateIn(), 3);
        assertEquals(timeAverageBrokerData.getLongTermMsgRateOut(), 4);
    }

    @Test
    public void testResetMethodOfMultipleBundles() {
        TimeAverageBrokerData timeAverageBrokerData = new TimeAverageBrokerData();
        Set<String> bundles = new HashSet<>();
        bundles.add("a");
        bundles.add("c");
        Map<String, BundleData> data = new HashMap<>();
        TimeAverageMessageData timeAverageMessageData = new TimeAverageMessageData();
        timeAverageMessageData.setMsgThroughputIn(1);
        timeAverageMessageData.setMsgThroughputOut(2);
        timeAverageMessageData.setMsgRateIn(3);
        timeAverageMessageData.setMsgRateOut(4);
        BundleData bundleData = new BundleData();
        bundleData.setLongTermData(timeAverageMessageData);
        bundleData.setShortTermData(timeAverageMessageData);
        data.put("a", bundleData);
        data.put("c", bundleData);
        timeAverageBrokerData.reset(bundles, data, null);
        assertEquals(timeAverageBrokerData.getShortTermMsgThroughputIn(), 2);
        assertEquals(timeAverageBrokerData.getShortTermMsgThroughputOut(), 4);
        assertEquals(timeAverageBrokerData.getShortTermMsgRateIn(), 6);
        assertEquals(timeAverageBrokerData.getShortTermMsgRateOut(), 8);
        assertEquals(timeAverageBrokerData.getLongTermMsgThroughputIn(), 2);
        assertEquals(timeAverageBrokerData.getLongTermMsgThroughputOut(), 4);
        assertEquals(timeAverageBrokerData.getLongTermMsgRateIn(), 6);
        assertEquals(timeAverageBrokerData.getLongTermMsgRateOut(), 8);
    }

}