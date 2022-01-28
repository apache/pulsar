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
package org.apache.pulsar.io.core;

import java.util.Map;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.functions.api.Record;

/**
 * Generic sink interface users can implement to run Sink on top of Pulsar Functions.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Sink<T> extends AutoCloseable {
    /**
     * Open connector with configuration.
     *
     * @param config initialization config
     * @param sinkContext environment where the sink connector is running
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(Map<String, Object> config, SinkContext sinkContext) throws Exception;

    /**
     * Write a message to Sink.
     *
     * @param record record to write to sink
     * @throws Exception
     */
    void write(Record<T> record) throws Exception;
}
