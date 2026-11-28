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
package org.apache.pulsar.functions.instance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.FunctionRecord;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.source.PulsarFunctionRecord;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SinkRecordTest {

    @Test
    public void testDeliveryTimeDefaultsToEmpty() {
        Record<String> legacy = () -> "value";
        assertThat(legacy.getDeliverAtTime()).isEmpty();
        assertThat(new SinkRecord<>(legacy, "output").getDeliverAtTime()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDeliveryTimeWrapperPrecedence() {
        Record<String> source = mock(Record.class);
        Record<String> output = mock(Record.class);
        when(source.getDeliverAtTime()).thenReturn(Optional.of(1000L));
        when(output.getDeliverAtTime()).thenReturn(Optional.of(2000L));
        assertThat(new SinkRecord<>(source, "value").getDeliverAtTime()).contains(1000L);
        assertThat(new PulsarFunctionRecord<>(source, new FunctionDetails()).getDeliverAtTime()).contains(1000L);
        assertThat(new OutputRecordSinkRecord<>(source, output).getDeliverAtTime()).contains(2000L);
        assertThat(new OutputRecordSinkRecord<>(source, output, "decoded", Schema.STRING).getDeliverAtTime())
                .contains(2000L);
        when(output.getDeliverAtTime()).thenReturn(Optional.empty());
        assertThat(new OutputRecordSinkRecord<>(source, output).getDeliverAtTime()).isEmpty();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFunctionRecordBuilderInheritsOverridesAndClearsDeliveryTime() {
        Context context = mock(Context.class);
        Record<String> source = mock(Record.class);
        when(source.getDeliverAtTime()).thenReturn(Optional.of(1000L));
        doReturn(source).when(context).getCurrentRecord();
        assertThat(FunctionRecord.from(context, Schema.STRING).build().getDeliverAtTime()).contains(1000L);
        assertThat(FunctionRecord.from(context, Schema.STRING).deliverAtTime(2000L).build().getDeliverAtTime())
                .contains(2000L);
        assertThat(FunctionRecord.from(context, Schema.STRING).deliverAtTime(null).build().getDeliverAtTime())
                .isEmpty();
        when(source.getDeliverAtTime()).thenReturn(Optional.empty());
        assertThat(FunctionRecord.from(context, Schema.STRING).build().getDeliverAtTime()).isEmpty();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testCustomAck() {

        PulsarRecord pulsarRecord = Mockito.mock(PulsarRecord.class);
        SinkRecord sinkRecord = new SinkRecord(pulsarRecord, new Object());

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
