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

package org.apache.pulsar.client.api;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * {@link TableViewBuilder} is used to configure and create instances of {@link TableView}.
 *
 * @see PulsarClient#newTableViewBuilder(Schema) ()
 *
 * @since 2.10.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TableViewBuilder<T> {

    /**
     * Load the configuration from provided <tt>config</tt> map.
     *
     *  <p>Example:
     *
     *  <pre>{@code
     *  Map<String, Object> config = new HashMap<>();
     *  config.put("topicName", "test-topic");
     *  config.put("autoUpdatePartitionsSeconds", "300");
     *
     *  TableViewBuilder<byte[]> builder = ...;
     *  builder = builder.loadConf(config);
     *
     *  TableView<byte[]> tableView = builder.create();
     *  }</pre>
     *
     * @param config configuration to load
     * @return the {@link TableViewBuilder} instance
     */
    TableViewBuilder<T> loadConf(Map<String, Object> config);

    /**
     * Finalize the creation of the {@link TableView} instance.
     *
     * <p>This method will block until the tableView is created successfully or an exception is thrown.
     *
     * @return the {@link TableView} instance
     * @throws PulsarClientException
     *              if the tableView creation fails
     */
    TableView<T> create() throws PulsarClientException;

    /**
     * Finalize the creation of the {@link TableView} instance in asynchronous mode.
     *
     *  <p>This method will return a {@link CompletableFuture} that can be used to access the instance when it's ready.
     *
     * @return the {@link TableView} instance
     */
    CompletableFuture<TableView<T>> createAsync();

    /**
     * Set the topic name of the {@link TableView}.
     *
     * @param topic the name of the topic to create the {@link TableView}
     * @return the {@link TableViewBuilder} builder instance
     */
    TableViewBuilder<T> topic(String topic);

    /**
     * Set the interval of updating partitions <i>(default: 1 minute)</i>.
     * @param interval the interval of updating partitions
     * @param unit the time unit of the interval
     * @return the {@link TableViewBuilder} builder instance
     */
    TableViewBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit);
}
