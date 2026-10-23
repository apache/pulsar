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
package org.apache.pulsar.functions.instance.v5;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.pulsar.client.api.MessageId;
import org.testng.annotations.Test;

public class V5MessageIdAdapterTest {

    @Test
    public void testJavaSerializationRoundTrip() throws Exception {
        org.apache.pulsar.client.api.v5.MessageId v5MessageId = org.apache.pulsar.client.api.v5.MessageId.latest();
        MessageId messageId = new V5MessageIdAdapter(v5MessageId);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(messageId);
        }
        Object restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = in.readObject();
        }

        assertThat(restored).isInstanceOf(V5MessageIdAdapter.class).isEqualTo(messageId);
        assertThat(((V5MessageIdAdapter) restored).toByteArray()).isEqualTo(v5MessageId.toByteArray());
    }
}
