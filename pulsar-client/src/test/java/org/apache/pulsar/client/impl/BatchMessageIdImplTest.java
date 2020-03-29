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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

public class BatchMessageIdImplTest {

    @Test
    public void compareToTest() {
        BatchMessageIdImpl batchMsgId1 = new BatchMessageIdImpl(0, 0, 0, 0);
        BatchMessageIdImpl batchMsgId2 = new BatchMessageIdImpl(1, 1, 1, 1);

        assertEquals(batchMsgId1.compareTo(batchMsgId2), -1);
        assertEquals(batchMsgId2.compareTo(batchMsgId1), 1);
        assertEquals(batchMsgId2.compareTo(batchMsgId2), 0);
    }

    @Test
    public void hashCodeTest() {
        BatchMessageIdImpl batchMsgId1 = new BatchMessageIdImpl(0, 0, 0, 0);
        BatchMessageIdImpl batchMsgId2 = new BatchMessageIdImpl(1, 1, 1, 1);

        assertEquals(batchMsgId1.hashCode(), batchMsgId1.hashCode());
        assertTrue(batchMsgId1.hashCode() != batchMsgId2.hashCode());
    }

    @Test
    public void equalsTest() {
        BatchMessageIdImpl batchMsgId1 = new BatchMessageIdImpl(0, 0, 0, 0);
        BatchMessageIdImpl batchMsgId2 = new BatchMessageIdImpl(1, 1, 1, 1);
        BatchMessageIdImpl batchMsgId3 = new BatchMessageIdImpl(0, 0, 0, 1);
        BatchMessageIdImpl batchMsgId4 = new BatchMessageIdImpl(0, 0, 0, -1);
        MessageIdImpl msgId = new MessageIdImpl(0, 0, 0);

        assertEquals(batchMsgId1, batchMsgId1);
        assertNotEquals(batchMsgId2, batchMsgId1);
        assertNotEquals(batchMsgId3, batchMsgId1);
        assertNotEquals(batchMsgId4, batchMsgId1);
        assertNotEquals(msgId, batchMsgId1);

        assertEquals(msgId, msgId);
        assertNotEquals(batchMsgId1, msgId);
        assertNotEquals(batchMsgId2, msgId);
        assertNotEquals(batchMsgId3, msgId);
        assertEquals(batchMsgId4, msgId);

        assertEquals(msgId, batchMsgId4);
    }

    @Test
    public void deserializationTest() {
        // initialize BitSet with null
        BatchMessageAcker ackerDisabled = new BatchMessageAcker(null, 0);
        BatchMessageIdImpl batchMsgId = new BatchMessageIdImpl(0, 0, 0, 0, ackerDisabled);

        ObjectWriter writer = ObjectMapperFactory.create().writerWithDefaultPrettyPrinter();

        try {
            writer.writeValueAsString(batchMsgId);
            fail("Shouldn't be deserialized");
        } catch (JsonProcessingException e) {
            // expected
            assertTrue(e.getCause() instanceof NullPointerException);
        }

        // use the default BatchMessageAckerDisabled
        BatchMessageIdImpl batchMsgIdToDeserialize = new BatchMessageIdImpl(0, 0, 0, 0);

        try {
            writer.writeValueAsString(batchMsgIdToDeserialize);
        } catch (JsonProcessingException e) {
            fail("Should be successful");
        }
    }

}
