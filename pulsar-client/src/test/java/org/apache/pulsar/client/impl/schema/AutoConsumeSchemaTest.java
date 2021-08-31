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
package org.apache.pulsar.client.impl.schema;

import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * AutoConsumeSchema test.
 */
@Slf4j
public class AutoConsumeSchemaTest {

    @Test
    public void decodeDataWithNullSchemaVersion() {
        Schema<GenericRecord> autoConsumeSchema = new AutoConsumeSchema();
        byte[] bytes = "bytes data".getBytes();
        MessageImpl<GenericRecord> message = MessageImpl.create(
                new MessageMetadata(), ByteBuffer.wrap(bytes), autoConsumeSchema, null);
        Assert.assertNull(message.getSchemaVersion());
        GenericRecord genericRecord = message.getValue();
        Assert.assertEquals(genericRecord.getNativeObject(), bytes);
    }

}
