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

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.api.proto.MessageIdData;

/**
 * A representation of a message in a topic in its raw form (i.e. as it is stored in a managed ledger).
 * RawMessages hold a refcount to the contains ByteBuf, so they must be closed for the ByteBuf to be freed.
 */
public interface RawMessage extends AutoCloseable {
    /**
     * Get the message ID of this message.
     */
    MessageId getMessageId();

    /**
     * Get the protobuf representation of the message ID of this message.
     */
    MessageIdData getMessageIdData();

    /**
     * Get a ByteBuf which contains the headers and payload of the message.
     * The payload may be compressed and encrypted, but whether this is the case can be verified
     * by decoding the headers which are not.
     */
    ByteBuf getHeadersAndPayload();

    /**
     * Serialize a raw message to a ByteBuf. The caller is responsible for releasing
     * the returned ByteBuf.
     */
    ByteBuf serialize();

    @Override
    void close();
}
