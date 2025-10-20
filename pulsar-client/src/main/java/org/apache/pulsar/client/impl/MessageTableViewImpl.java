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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;

@Slf4j
public class MessageTableViewImpl<T> extends AbstractTableViewImpl<T, Message<T>> {
    MessageTableViewImpl(PulsarClientImpl client, Schema<T> schema, TableViewConfigurationData conf) {
        super(client, schema, conf);
    }

    @Override
    protected void maybeReleaseMessage(Message<T> msg) {
        // don't release the message. Pooling of messages might have to be disabled in the client when using
        // MessageTableViewImpl.
    }

    @Override
    protected Message<T> getValueIfPresent(Message<T> msg) {
        // return the message as the value for the table view
        // if the payload is empty, the message is considered a tombstone message and it won't be preserved in the map
        return msg.size() > 0 ? msg : null;
    }
}