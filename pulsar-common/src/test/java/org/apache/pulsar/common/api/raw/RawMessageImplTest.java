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
package org.apache.pulsar.common.api.raw;

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class RawMessageImplTest {

    private static final String HARD_CODE_KEY = "__pfn_input_topic__";
    private static final String KEY_VALUE_FIRST= "persistent://first-tenant-value/first-namespace-value/first-topic-value";
    private static final String KEY_VALUE_SECOND = "persistent://second-tenant-value/second-namespace-value/second-topic-value";
    private static final String HARD_CODE_KEY_ID = "__pfn_input_msg_id__";
    private static final String HARD_CODE_KEY_ID_VALUE  = "__pfn_input_msg_id_value__";

    @Test
    public void testGetProperties() {
        ReferenceCountedMessageMetadata refCntMsgMetadata =
                ReferenceCountedMessageMetadata.get(Mockito.mock(ByteBuf.class));
        SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
        singleMessageMetadata.addProperty().setKey(HARD_CODE_KEY).setValue(KEY_VALUE_FIRST);
        singleMessageMetadata.addProperty().setKey(HARD_CODE_KEY).setValue(KEY_VALUE_SECOND);
        singleMessageMetadata.addProperty().setKey(HARD_CODE_KEY_ID).setValue(HARD_CODE_KEY_ID_VALUE);
        RawMessage msg = RawMessageImpl.get(refCntMsgMetadata, singleMessageMetadata, null , 0, 0, 0);
        Map<String, String> properties = msg.getProperties();
        assertEquals(properties.get(HARD_CODE_KEY), KEY_VALUE_SECOND);
        assertEquals(properties.get(HARD_CODE_KEY_ID), HARD_CODE_KEY_ID_VALUE);
        assertEquals(KEY_VALUE_SECOND, properties.get(HARD_CODE_KEY));
        assertEquals(HARD_CODE_KEY_ID_VALUE, properties.get(HARD_CODE_KEY_ID));
    }
}