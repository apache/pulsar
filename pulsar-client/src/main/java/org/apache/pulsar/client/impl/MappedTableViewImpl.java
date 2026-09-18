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
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TableViewMessageMapper;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;

/**
 * {@link TableView} implementation that applies a user-provided {@link TableViewMessageMapper} to each
 * message to produce the value stored in the view.
 *
 * @param <T> the message schema type
 * @param <V> the value type returned by the mapper
 */
class MappedTableViewImpl<T, V> extends AbstractTableViewImpl<T, V> {

    static final String COMPACTION_STRATEGY_UNSUPPORTED =
            "topicCompactionStrategyClassName is not supported for mapped table views: a "
                    + "TopicCompactionStrategy compares values of the topic's schema type, not the mapper's "
                    + "output type";

    private final TableViewMessageMapper<T, V> mapper;

    MappedTableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf,
                        TableViewMessageMapper<T, V> mapper) {
        // The message instance is passed to the user-provided mapper, which may keep a reference to it
        // (e.g. when "msg -> msg" is used as the mapper). Pooled messages must not be used since there is
        // no way to know when the message could be released.
        super(client, schema, requireNoCompactionStrategy(conf), false);
        this.mapper = mapper;
    }

    /**
     * A {@link TopicCompactionStrategy} compares values of the topic's schema type, which a mapped view does
     * not store, so the pair would fail with a {@code ClassCastException} on the reader thread. The builder
     * rejects it up front; this guard keeps the invariant with the class that depends on it and runs before
     * the superclass constructor creates the reader.
     */
    private static TableViewConfigurationData requireNoCompactionStrategy(TableViewConfigurationData conf) {
        checkArgument(conf.getTopicCompactionStrategyClassName() == null, COMPACTION_STRATEGY_UNSUPPORTED);
        return conf;
    }

    @Override
    protected V getValue(Message<T> msg) throws Exception {
        return mapper.map(msg);
    }

    @Override
    protected boolean onMappingError(Message<T> msg, Throwable error) {
        try {
            return mapper.onMappingError(msg, error);
        } catch (Throwable t) {
            log.error().attr("key", msg.getKey())
                    .attr("messageId", msg.getMessageId())
                    .exception(t)
                    .log("Table view mapper error callback raised an exception");
            return false;
        }
    }
}
