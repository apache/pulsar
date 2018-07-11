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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.nio.ByteBuffer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.testng.annotations.Test;

/**
 * Unit test of {@link MessageImpl}.
 */
public class MessageImplTest {

    @Test
    public void testGetSequenceIdNotAssociated() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals(-1, msg.getSequenceId());
    }

    @Test
    public void testGetSequenceIdAssociated() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
            .setSequenceId(1234);

        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals(1234, msg.getSequenceId());
    }

    @Test
    public void testGetProducerNameNotAssigned() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertNull(msg.getProducerName());
    }

    @Test
    public void testGetProducerNameAssigned() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
            .setProducerName("test-producer");

        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals("test-producer", msg.getProducerName());
    }

}
