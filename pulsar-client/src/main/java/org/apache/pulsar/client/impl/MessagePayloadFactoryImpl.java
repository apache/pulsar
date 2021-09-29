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
package org.apache.pulsar.client.impl;

import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.MessagePayload;
import org.apache.pulsar.client.api.MessagePayloadFactory;

public class MessagePayloadFactoryImpl implements MessagePayloadFactory {

    @Override
    public MessagePayload wrap(byte[] bytes) {
        return MessagePayloadImpl.create(Unpooled.wrappedBuffer(bytes));
    }

    @Override
    public MessagePayload wrap(ByteBuffer buffer) {
        return MessagePayloadImpl.create(Unpooled.wrappedBuffer(buffer));
    }
}
