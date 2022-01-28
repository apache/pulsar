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
import java.util.function.Consumer;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.functions.api.Record;

/**
 * Interface for writing Batch sources
 *
 * The lifecycle of the BatchSource is as follows
 * 1. open - called once when connector is started.  Can use method to perform
 *    certain one-time operations such as init/setup operations. This is called on all
 *    instances of the source and is analogous to the open method of the streaming Source api.
 * 2. discover (called only on one instance (Currently instance zero(0), but might change later))
 *    - The discovery phase will be executed on one instance of the connector.
 *    - discover is triggered by the BatchSourceTriggerer class configured for this source.
 *    - As and when discover discovers new tasks, it will emit them using the taskEater method.
 *    - The framework will distribute the discovered tasks among all instances
 * 3. prepare - is called on an instance when there is a new discovered task assigned for that instance
 *    - The framework decides which discovered task is routed to which source instance. The connector
 *      does not currently have a way to influence this.
 *    - prepare is only called when the instance has fetched all records using readNext for its previously
 *      assigned discovered task.
 * 4. readNext is called repeatedly by the framework to fetch the next record. If there are no
 *             more records available to emit, the connector should return null. That indicates
 *             that all records for that particular discovered task is complete.
 * 5. close is called when the source is stopped/deleted. This is analogous to the streaming Source api.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface BatchSource<T> extends AutoCloseable {

    /**
     * Open connector with configuration.
     *
     * @param config config that's supplied for source
     * @param context environment where the source connector is running
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(Map<String, Object> config, SourceContext context) throws Exception;

    /**
     * Discovery phase of a connector.  This phase will only be run on one instance, i.e. instance 0, of the connector.
     * Implementations use the taskEater consumer to output serialized representation of tasks as they are discovered.
     *
     * @param taskEater function to notify the framework about the new task received.
     * @throws Exception during discover
     */
    void discover(Consumer<byte[]> taskEater) throws Exception;

    /**
     * Called when a new task appears for this connector instance.
     *
     * @param task the serialized representation of the task
     */
    void prepare(byte[] task) throws Exception;

    /**
     * Read data and return a record.
     * Return null if no more records are present for this task
     * @return a record
     */
    Record<T> readNext() throws Exception;
}