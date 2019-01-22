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
package org.apache.pulsar.io.nifi;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.SourceContext;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * NiFiSink and NiFiSource test
 */
public class NiFiSinkAndSourceTest {
    @Mock
    protected SinkContext mockSinkContext;
    @Mock
    protected SourceContext mockSourceContext;

    private String msg = "Data from Pulsar";
    private Map<String, Object> map;

    @Test
    public void TestSinkAndSource() throws Exception {
        map = new HashMap<>();
        map.put("url","http://localhost:8080/nifi");
        map.put("portName","Data from Pulsar");
        map.put("requestBatchCount",1);
        map.put("waitTimeMs",10);

        NiFiSink sink = new NiFiSink();
        mockSinkContext = mock(SinkContext.class);
        sink.open(map, mockSinkContext);

        Record<NiFiDataPacket> mockRecord = mock(Record.class);
        when(mockRecord.getValue()).thenAnswer(new Answer<NiFiDataPacket>() {
            public NiFiDataPacket answer(InvocationOnMock invocation) throws Throwable {
                return new StandardNiFiDataPacket(msg.getBytes(), new HashMap<String, String>());
            }});

        sink.write(mockRecord);

        // msg has been send to nifi Input Port, read it from Output Port and verify.
        NiFiSource source = new NiFiSource();
        map.put("portName","Data to Pulsar");
        mockSourceContext = mock(SourceContext.class);
        source.open(map, mockSourceContext);

        Record<NiFiDataPacket> readRecord = source.read();
        NiFiDataPacket recordValue = readRecord.getValue();
        Assert.assertEquals(msg, new String(recordValue.getContent()));
        Assert.assertNotNull(recordValue.getAttributes());
    }
}
