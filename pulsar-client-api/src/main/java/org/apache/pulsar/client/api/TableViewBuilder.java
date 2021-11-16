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

public interface TableViewBuilder<T> {

    TableViewBuilder<T> loadConf(Map<String, Object> config);

    TableView<T> create() throws PulsarClientException;

    CompletableFuture<TableView<T>> createAsync();

    TableViewBuilder<T> topic(String topic);

    /**
     * Set the interval of updating partitions <i>(default: 1 minute)</i>
     * @param interval the interval of updating partitions
     * @param unit the time unit of the interval
     * @return the consumer builder instance
     */
    TableViewBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit);
}
