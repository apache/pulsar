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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.service.AbstractDispatcherSingleActiveConsumer.balancedAssignment;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BalancedActiveConsumerSelectionTest {

    private static final String TOPIC = "persistent://public/default/balanced";

    @Test
    public void testEveryConsumerGetsItsQuota() {
        for (int partitions : new int[]{1, 3, 4, 10, 12, 64, 100}) {
            for (int n : new int[]{1, 3, 4, 8, 16}) {
                int[] loads = loads(balancedAssignment(TOPIC, partitions, consumers(n)), n);
                int base = partitions / n;
                assertTrue(Arrays.stream(loads).allMatch(load -> load == base || load == base + 1),
                        partitions + " partitions, " + n + " consumers: " + Arrays.toString(loads));
                assertEquals(Arrays.stream(loads).filter(load -> load == base + 1).count(), partitions % n);
            }
        }
    }

    @Test
    public void testConsumerIndexesSharingANameGetTheirQuotas() {
        // Index quotas imply instance balance only if each index identifies the same consumer on every partition.
        assertEquals(loads(balancedAssignment(TOPIC, 6, List.of("app", "app", "app")), 3), new int[]{2, 2, 2});
    }

    @Test
    public void testPartitionZeroOfDifferentTopicsLandsOnDifferentConsumers() {
        // The modulo assignment puts partition 0 of every topic on the same consumer
        Set<Integer> owners = new HashSet<>();
        for (int t = 0; t < 20; t++) {
            owners.add(balancedAssignment(TOPIC + "-" + t, 4, consumers(4))[0]);
        }
        assertEquals(owners.size(), 4);
    }

    @Test
    public void testConsumerChangeMovesFewPartitions() {
        List<String> consumers = consumers(8);
        List<String> before = owners(64, consumers);

        List<String> added = new ArrayList<>(consumers);
        added.add("consumer-new");
        // The modulo assignment moves 56 of the 64 partitions when the 9th consumer joins
        assertTrue(moved(before, owners(64, added)) <= 16);
        assertTrue(moved(before, owners(64, consumers.subList(1, 8))) <= 16);
    }

    private static int[] loads(int[] assignment, int consumers) {
        int[] loads = new int[consumers];
        for (int c : assignment) {
            loads[c]++;
        }
        return loads;
    }

    private static List<String> owners(int partitions, List<String> consumers) {
        return Arrays.stream(balancedAssignment(TOPIC, partitions, consumers)).mapToObj(consumers::get).toList();
    }

    private static long moved(List<String> before, List<String> after) {
        return IntStream.range(0, before.size()).filter(p -> !before.get(p).equals(after.get(p))).count();
    }

    private static List<String> consumers(int n) {
        return IntStream.range(0, n).mapToObj(i -> "consumer-" + i).toList();
    }
}
