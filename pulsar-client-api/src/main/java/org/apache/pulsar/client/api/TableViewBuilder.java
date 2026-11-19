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
     * Creates a {@link TableView} instance where the values are produced by a user-defined
     * {@link TableViewMessageMapper mapper} from each message.
     *
     * <p>This provides a flexible way to create a key-value view over a topic, allowing users to extract data
     * from the message payload, properties, and other metadata into a custom object of type {@code V}. To get
     * a view of the full {@link Message} objects, use {@code msg -> msg} as the mapper. Message pooling is not
     * used for mapped table views, so it is safe to keep a reference to the {@link Message} instance passed to
     * the mapper. A retained {@link Message} holds more than its payload, though: it also keeps its metadata,
     * schema and a reference to the connection it was received on. For a topic with many keys, prefer a
     * mapper that copies the needed fields into a value object over {@code msg -> msg}.
     *
     * <p>A keyed message with an empty payload is a tombstone: the key is removed from the view and the mapper
     * is not called for it. If the mapper returns {@code null}, the message is also treated as a tombstone.
     * If the mapper throws, the message is skipped and {@link TableViewMessageMapper#onMappingError} is
     * called; the key keeps its previous value.
     *
     * <p>A {@code topicCompactionStrategyClassName} loaded with {@link #loadConf(Map)} is rejected for
     * mapped table views: a {@code TopicCompactionStrategy} compares values of the topic's schema type, which
     * a mapped table view does not store.
     *
     * @param mapper the mapper that produces the value of type {@code V} for each {@link Message}
     * @param <V> the type of the values in the {@link TableView}
     * @return the {@link TableView} instance
     * @throws PulsarClientException
     *              if the tableView creation fails
     * @throws IllegalArgumentException
     *              if the mapper is {@code null} or a topic compaction strategy is configured
     */
    <V> TableView<V> createMapped(TableViewMessageMapper<T, V> mapper) throws PulsarClientException;

    /**
     * Creates a {@link TableView} instance in asynchronous mode where the values are produced by a
     * user-defined {@link TableViewMessageMapper mapper} from each message.
     *
     * <p>See {@link #createMapped(TableViewMessageMapper)} for the mapping contract.
     *
     * @param mapper the mapper that produces the value of type {@code V} for each {@link Message}
     * @param <V> the type of the values in the {@link TableView}
     * @return a future that can be used to access the {@link TableView} instance when it's ready; it fails
     *         with {@link IllegalArgumentException} if the mapper is {@code null} or a topic compaction
     *         strategy is configured
     */
    <V> CompletableFuture<TableView<V>> createMappedAsync(TableViewMessageMapper<T, V> mapper);

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


    /**
     * Set the subscription name of the {@link TableView}.
     *
     * @param subscriptionName the name of the subscription to the topic
     * @return the {@link TableViewBuilder} builder instance
     */
    TableViewBuilder<T> subscriptionName(String subscriptionName);

    /**
     * Set the {@link CryptoKeyReader} to decrypt the message payloads.
     *
     * @param cryptoKeyReader CryptoKeyReader object
     * @return the {@link TableViewBuilder} builder instance
     */
    TableViewBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader);

    /**
     * Set the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt message payloads.
     *
     * @param privateKey the private key that is always used to decrypt message payloads.
     * @return the {@link TableViewBuilder} builder instance
     */
    TableViewBuilder<T> defaultCryptoKeyReader(String privateKey);

    /**
     * Set the default implementation of {@link CryptoKeyReader}.
     *
     * <p>Configure the key reader to be used to decrypt message payloads.
     *
     * @param privateKeys the map of private key names and their URIs
     *                    used to decrypt message payloads.
     * @return the {@link TableViewBuilder} builder instance
     */
    TableViewBuilder<T> defaultCryptoKeyReader(Map<String, String> privateKeys);

    /**
     * Set the {@link ConsumerCryptoFailureAction} to specify.
     *
     * @param action the action to take when the decoding fails
     * @return the {@link TableViewBuilder} builder instance
     */
    TableViewBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action);
}
