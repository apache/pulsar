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

import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

@Slf4j
public class MessageMapperTableView<T, V> extends AbstractTableView<T, V> {

    private final Function<Message<T>, V> mapper;

    MessageMapperTableView(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf,
                           Function<Message<T>, V> mapper) {
        super(client, schema, conf);
        this.mapper = mapper;
    }

    @Override
    protected void maybeReleaseMessage(Message<T> msg) {
        // The message is passed to the user-defined mapper function.
        // The user is responsible for releasing the message if needed.
        // To be safe, we don't release the message here.
    }

    @Override
    protected V getValueIfPresent(Message<T> msg) {
        return msg.size() > 0 ? mapper.apply(msg) : null;
    }
}
