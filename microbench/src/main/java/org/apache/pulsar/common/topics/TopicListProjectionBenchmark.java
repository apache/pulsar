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
package org.apache.pulsar.common.topics;

import com.google.re2j.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pulsar.common.naming.TopicName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares the previous regex split with literal projection, both alone and in a complete topic filter.
 * Default parameters target the large sparse namespace; override selection, engine and names for boundary runs.
 * Every operation processes one complete list, so allocation/op is allocation per list, not per topic.
 * The filter candidate calls production code; the projection candidate isolates its literal-string mechanism.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 3, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Threads(1)
public class TopicListProjectionBenchmark {
    private static final String SEPARATOR = "://";
    private static final Pattern SEPARATOR_PATTERN = Pattern.compile(Pattern.quote(SEPARATOR));

    @Param({"100000", "200000"})
    public int topicCount;

    @Param({"sparse"})
    public String selection;

    @Param({"RE2J"})
    public String engine;

    @Param({"plain"})
    public String names;

    private List<String> topics;
    private List<String> canonicalBases;
    private TopicsPattern topicsPattern;

    @Setup(Level.Trial)
    public void setup() {
        topics = new ArrayList<>(topicCount);
        canonicalBases = new ArrayList<>(topicCount);
        for (int i = 0; i < topicCount; i++) {
            String local = "t" + String.format(Locale.ROOT, "%06d", i);
            local += switch (names) {
                case "plain" -> "";
                case "partitioned" -> "-partition-0";
                case "embedded" -> "://tail";
                case "unicode" -> "-雪";
                default -> throw new IllegalArgumentException("Unknown name set: " + names);
            };
            String domain = names.equals("embedded") ? "topic" : "persistent";
            String topic = domain + "://public/bench/" + local;
            TopicName canonical = TopicName.get(topic); // Warm the shared name cache outside measured operations.
            topics.add(topic);
            String base = canonical.getPartitionedTopicName();
            canonicalBases.add(base);
            if (!SEPARATOR_PATTERN.split(base)[1].equals(literalProjection(base))) {
                throw new IllegalStateException("Projection differs from the previous implementation: " + base);
            }
        }
        String expression = switch (selection) {
            // Matches the overlapping pattern of one subscription: 40/80 results per 100k/200k topics.
            case "sparse" -> "public/bench/t[0-9]{2}[05]00[01]" + (names.equals("unicode") ? "-雪" : "");
            case "all" -> ".*";
            case "none" -> "public/bench/absent-.+";
            default -> throw new IllegalArgumentException("Unknown selection: " + selection);
        };
        topicsPattern = TopicsPatternFactory.create(expression, TopicsPattern.RegexImplementation.valueOf(engine));
        if (!legacyFilter().equals(TopicList.filterTopics(topics, topicsPattern))) {
            throw new IllegalStateException("Full filter differs from the previous implementation");
        }
    }

    @Benchmark
    public void regexSplitProjection(Blackhole blackhole) {
        for (String base : canonicalBases) {
            blackhole.consume(SEPARATOR_PATTERN.split(base)[1]);
        }
    }

    @Benchmark
    public void literalProjection(Blackhole blackhole) {
        for (String base : canonicalBases) {
            blackhole.consume(literalProjection(base));
        }
    }

    @Benchmark
    public List<String> regexSplitFullFilter() {
        return legacyFilter();
    }

    @Benchmark
    public List<String> productionFullFilter() {
        return TopicList.filterTopics(topics, topicsPattern);
    }

    private List<String> legacyFilter() {
        // Keep the old pipeline and collector, changing only the projected string under comparison.
        return topics.stream().map(TopicName::get).filter(topic -> topicsPattern.matches(
                        SEPARATOR_PATTERN.split(topic.getPartitionedTopicName())[1]))
                .map(TopicName::toString).collect(Collectors.toList());
    }

    private static String literalProjection(String base) {
        int start = base.indexOf(SEPARATOR) + SEPARATOR.length();
        int end = base.indexOf(SEPARATOR, start);
        return end < 0 ? base.substring(start) : base.substring(start, end);
    }
}
