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
package org.apache.pulsar.broker.stats;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

/**
 */
public class TopicLoadStats {

    /** Statistics for topic load times **/
    public double meanTopicLoadMs;

    public double medianTopicLoadMs;

    public double topicLoad95Ms;

    public double topicLoad99Ms;

    public double topicLoad999Ms;

    public double topicsLoad9999Ms;

    public double topicLoadCounts;

    public double elapsedIntervalMs;

    private Recorder topicLoadTimeRecorder = new Recorder(TimeUnit.MINUTES.toMillis(10), 2);
    private Histogram topicLoadHistogram = null;
    private double topicLoadRecordStartTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

    public void updateStats() {

        topicLoadHistogram = topicLoadTimeRecorder.getIntervalHistogram(topicLoadHistogram);
        this.elapsedIntervalMs = (TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - topicLoadRecordStartTime);
        topicLoadRecordStartTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

        this.meanTopicLoadMs = topicLoadHistogram.getMean();
        this.medianTopicLoadMs = topicLoadHistogram.getValueAtPercentile(50);
        this.topicLoad95Ms = topicLoadHistogram.getValueAtPercentile(95);
        this.topicLoad99Ms = topicLoadHistogram.getValueAtPercentile(99);
        this.topicLoad999Ms = topicLoadHistogram.getValueAtPercentile(99.9);
        this.topicsLoad9999Ms = topicLoadHistogram.getValueAtPercentile(99.99);
        this.topicLoadCounts = topicLoadHistogram.getTotalCount();
    }

    public void recordTopicLoadTimeValue(long topicLoadLatencyMs) {
        topicLoadTimeRecorder.recordValue(topicLoadLatencyMs);
    }
}
