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
package org.apache.pulsar.functions.instance;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SinkRecordTest {

    @Test
    public void testCustomAck() {

        PulsarRecord pulsarRecord = Mockito.mock(PulsarRecord.class);
        SinkRecord sinkRecord = new SinkRecord<>(pulsarRecord, new Object());

        sinkRecord.cumulativeAck();
        Mockito.verify(pulsarRecord, Mockito.times(1)).cumulativeAck();

        sinkRecord = new SinkRecord(Mockito.mock(Record.class), new Object());
        try {
            sinkRecord.individualAck();
            fail("Should throw runtime exception");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertEquals(e.getMessage(), "SourceRecord class type must be PulsarRecord");
        }
    }
}