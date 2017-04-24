/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.pulsar.client.api;

import org.testng.annotations.Test;

import com.google.common.base.Objects;
import com.yahoo.pulsar.client.impl.BatchMessageIdImpl;
import com.yahoo.pulsar.client.impl.ConsumerId;
import com.yahoo.pulsar.client.impl.MessageIdImpl;

import static org.testng.Assert.assertEquals;

import org.testng.Assert;

public class MessageIdTest {
    
    @Test
    public void messageIdTest() {
        MessageId mId = new MessageIdImpl(1, 2, 3);
        assertEquals(mId.toString(), "MessageIdImpl = 1:2:3");
        
        mId = new BatchMessageIdImpl(0, 2, 3, 4);
        assertEquals(mId.toString(), "BatchMessageIdImpl = 0:2:3:4");
        
        mId = new BatchMessageIdImpl(-1, 2, -3, 4);
        assertEquals(mId.toString(), "BatchMessageIdImpl = -1:2:-3:4");
        
        mId = new MessageIdImpl(0, -23, 3);
        assertEquals(mId.toString(), "MessageIdImpl = 0:-23:3");
    }
}
