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
package org.apache.pulsar.functions.source;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.proto.Function;
import org.testng.annotations.Test;

public class PulsarFunctionRecordTest {

    @Test
    public void testAck() {
        Record record = mock(Record.class);
        Function.FunctionDetails functionDetails = Function.FunctionDetails.newBuilder().setAutoAck(true)
                .setProcessingGuarantees(Function.ProcessingGuarantees.ATMOST_ONCE).build();
        PulsarFunctionRecord pulsarFunctionRecord = new PulsarFunctionRecord<>(record, functionDetails);
        pulsarFunctionRecord.ack();
        verify(record, times(0)).ack();

        clearInvocations(record);
        functionDetails = Function.FunctionDetails.newBuilder().setAutoAck(true)
                .setProcessingGuarantees(Function.ProcessingGuarantees.ATLEAST_ONCE).build();
        pulsarFunctionRecord = new PulsarFunctionRecord<>(record, functionDetails);
        pulsarFunctionRecord.ack();
        verify(record, times(0)).ack();

        clearInvocations(record);
        functionDetails = Function.FunctionDetails.newBuilder().setAutoAck(true)
                .setProcessingGuarantees(Function.ProcessingGuarantees.EFFECTIVELY_ONCE).build();
        pulsarFunctionRecord = new PulsarFunctionRecord<>(record, functionDetails);
        pulsarFunctionRecord.ack();
        verify(record, times(0)).ack();

        clearInvocations(record);
        functionDetails = Function.FunctionDetails.newBuilder().setAutoAck(true)
                .setProcessingGuarantees(Function.ProcessingGuarantees.MANUAL).build();
        pulsarFunctionRecord = new PulsarFunctionRecord<>(record, functionDetails);
        pulsarFunctionRecord.ack();
        verify(record, times(1)).ack();
    }
}
