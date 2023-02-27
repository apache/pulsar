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
package org.apache.pulsar.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;

public class TableViewBuilderImpl<T> implements TableViewBuilder<T> {

    private final PulsarClientImpl client;
    private final Schema<T> schema;
    private TableViewConfigurationData conf;

    TableViewBuilderImpl(PulsarClientImpl client, Schema<T> schema) {
        this.client = client;
        this.schema = schema;
        this.conf = new TableViewConfigurationData();
    }

    @Override
    public TableViewBuilder<T> loadConf(Map<String, Object> config) {
        conf = ConfigurationDataUtils.loadData(
                config, conf, TableViewConfigurationData.class);
        return this;
    }

    @Override
    public TableView<T> create() throws PulsarClientException {
       try {
           return createAsync().get();
       } catch (Exception e) {
           throw PulsarClientException.unwrap(e);
       }
    }

    @Override
    public CompletableFuture<TableView<T>> createAsync() {
       return new TableViewImpl<>(client, schema, conf).start();
    }

    @Override
    public TableViewBuilder<T> topic(String topic) {
       checkArgument(StringUtils.isNotBlank(topic), "topic cannot be blank");
       conf.setTopicName(StringUtils.trim(topic));
       return this;
    }

    @Override
    public TableViewBuilder<T> autoUpdatePartitionsInterval(int interval, TimeUnit unit) {
       checkArgument(unit.toSeconds(interval) >= 1, "minimum is 1 second");
       conf.setAutoUpdatePartitionsSeconds(unit.toSeconds(interval));
       return this;
    }

    @Override
    public TableViewBuilder<T> subscriptionName(String subscriptionName) {
        checkArgument(StringUtils.isNotBlank(subscriptionName), "subscription name cannot be blank");
        conf.setSubscriptionName(StringUtils.trim(subscriptionName));
        return this;
    }

    @Override
    public TableViewBuilder<T> cryptoKeyReader(CryptoKeyReader cryptoKeyReader) {
        conf.setCryptoKeyReader(cryptoKeyReader);
        return this;
    }

    @Override
    public TableViewBuilder<T> defaultCryptoKeyReader(String privateKey) {
        checkArgument(StringUtils.isNotBlank(privateKey), "privateKey cannot be blank");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().defaultPrivateKey(privateKey).build());
    }

    @Override
    public TableViewBuilder<T> defaultCryptoKeyReader(@NonNull Map<String, String> privateKeys) {
        checkArgument(!privateKeys.isEmpty(), "privateKeys cannot be empty");
        return cryptoKeyReader(DefaultCryptoKeyReader.builder().privateKeys(privateKeys).build());
    }

    @Override
    public TableViewBuilder<T> cryptoFailureAction(ConsumerCryptoFailureAction action) {
        conf.setCryptoFailureAction(action);
        return this;
    }
}
