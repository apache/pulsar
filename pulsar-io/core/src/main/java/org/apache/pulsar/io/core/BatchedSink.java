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

import org.apache.pulsar.functions.api.Record;

import java.util.Collection;
import java.util.Map;

/**
 * A Batched version of the sink interface where the write method is called
 * only upon accumulation of a window of events
 */
public interface BatchedSink<T> extends AutoCloseable {
    /**
     * Open connector with configuration
     *
     * @param config initialization config
     * @param sinkContext
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(final Map<String, Object> config, SinkContext sinkContext) throws Exception;

    /**
     * Write a message to Sink
     * @param records The batch of records to be written to the sink
     * @throws Exception
     */
    void write(Collection<Record<T>> records) throws Exception;
}
