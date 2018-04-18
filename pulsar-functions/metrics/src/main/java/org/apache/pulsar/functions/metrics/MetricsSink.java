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
package org.apache.pulsar.functions.metrics;

import java.util.Map;

import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData;

/**
 * The metrics sink interface. <p>
 * Implementations of this interface consume the {@link MetricsData}
 * The calling entity pushes the {@link MetricsData} to the sink using
 * {@link #processRecord(MetricsData)} method.
 * And {@link #flush()} is called at an interval according to the configuration
 */
public interface MetricsSink extends AutoCloseable {
    /**
     * Initialize the MetricsSink
     *
     * @param conf An unmodifiableMap containing basic configuration
     * Attempts to modify the returned map,
     * whether direct or via its collection views, result in an UnsupportedOperationException.
     */
    void init(Map<String, String> conf);

    /**
     * Process a metrics record in the sink
     *  @param record the record to put
     * @param functionDetails functionDetails that generated this record
     */
    void processRecord(MetricsData record, Function.FunctionDetails functionDetails);

    /**
     * Flush any buffered metrics
     * It would be called at an interval according to the configuration
     */
    void flush();

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     */
    void close();
}
