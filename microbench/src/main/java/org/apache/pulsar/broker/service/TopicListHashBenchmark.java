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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.topics.TopicList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmarks {@link TopicList#calculateHash} with different set implementations,
 * mirroring how {@link org.apache.pulsar.broker.service.TopicListService.TopicListWatcher}
 * maintains matching topics: seeded in lexicographic order (MetadataStore.getChildren),
 * with occasional add/remove (simulated via the operation benchmarks).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class TopicListHashBenchmark {

    @Param({"1000", "10000", "100000"})
    public int topicCount;

    @Param({"partitioned", "nonPartitioned", "mixed"})
    public String topicKind;

    /** Seed topics in lexicographic order (matches MetadataStore.getChildren). */
    private List<String> seedTopics;

    private LinkedHashSet<String> linkedHashSet;
    private TreeSet<String> treeSet;

    @Setup
    public void setup() {
        seedTopics = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            String suffix = switch (topicKind) {
                case "partitioned" -> "-partition-" + (i % 16);
                case "nonPartitioned" -> "";
                case "mixed" -> (i % 3 == 0 ? "-partition-" + (i % 8) : "");
                default -> throw new IllegalArgumentException(topicKind);
            };
            seedTopics.add("persistent://tenant/ns/topic-" + i + suffix);
        }
        Collections.sort(seedTopics);

        linkedHashSet = new LinkedHashSet<>(seedTopics);
        treeSet = new TreeSet<>(seedTopics);
    }

    // --- Hash-only benchmarks ---

    /** Baseline: LinkedHashSet + Collection overload (original code path). */
    @Benchmark
    public String hashLinkedHashSetCollection() {
        return TopicList.calculateHash(linkedHashSet);
    }

    /** TreeSet + Collection overload (TreeSet is sorted, but still goes through copy+sort). */
    @Benchmark
    public String hashTreeSetCollection() {
        return TopicList.calculateHash((Set<String>) treeSet);
    }

    /** This PR's approach: TreeSet + SortedSet overload (no copy, no sort). */
    @Benchmark
    public String hashTreeSetSortedSet() {
        return TopicList.calculateHash((SortedSet<String>) treeSet);
    }

    // --- Per-event benchmarks: add/remove one topic, then hash ---
    // Mirrors TopicListWatcher.onTopicEvent add/remove + calculateHash.

    @Benchmark
    public String addAndHashLinkedHashSet() {
        // simulate add of a topic that wasn't there before (end of list)
        String newTopic = "persistent://tenant/ns/topic-" + topicCount + "-partition-0";
        linkedHashSet.add(newTopic);
        String hash = TopicList.calculateHash(linkedHashSet);
        linkedHashSet.remove(newTopic);
        return hash;
    }

    @Benchmark
    public String addAndHashTreeSet() {
        String newTopic = "persistent://tenant/ns/topic-" + topicCount + "-partition-0";
        treeSet.add(newTopic);
        String hash = TopicList.calculateHash((SortedSet<String>) treeSet);
        treeSet.remove(newTopic);
        return hash;
    }
}
