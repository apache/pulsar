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
package org.apache.pulsar.client.impl.v5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.testng.annotations.Test;

public class V5InteropTest {

    @Test
    public void testSchemaRoundTrip() {
        org.apache.pulsar.client.api.Schema<String> v4Schema = org.apache.pulsar.client.api.Schema.STRING;
        Schema<String> v5Schema = V5Interop.toV5Schema(v4Schema);
        assertThat(v5Schema.schemaInfo().type().name()).isEqualTo(v4Schema.getSchemaInfo().getType().name());
        assertThat(V5Interop.toV4Schema(v5Schema)).isSameAs(v4Schema);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testV4Message() {
        org.apache.pulsar.client.api.Message<String> v4Message = mock(org.apache.pulsar.client.api.Message.class);
        when(v4Message.getMessageId()).thenReturn(MessageId.earliest);
        MessageV5<String> v5Message = new MessageV5<>(v4Message, 1L);
        assertThat(V5Interop.v4Message(v5Message)).containsSame(v4Message);

        assertThat(V5Interop.v4Message(mock(Message.class))).isEmpty();
    }
}
