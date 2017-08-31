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

import static org.junit.Assert.assertEquals;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.junit.Test;

/**
 * Unit test of {@link MessageBuilderImpl}.
 */
public class MessageBuilderTest {

    @Test(expected = IllegalArgumentException.class)
    public void testSetEventTimeNegative() {
        MessageBuilder builder = MessageBuilder.create();
        builder.setEventTime(-1L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetEventTimeZero() {
        MessageBuilder builder = MessageBuilder.create();
        builder.setEventTime(0L);
    }

    @Test
    public void testSetEventTimePositive() {
        long eventTime = System.currentTimeMillis();
        MessageBuilder builder = MessageBuilder.create();
        builder.setContent(new byte[0]);
        builder.setEventTime(eventTime);
        Message msg = builder.build();
        assertEquals(eventTime, msg.getEventTime());
    }

    @Test
    public void testBuildMessageWithoutEventTime() {
        MessageBuilder builder = MessageBuilder.create();
        builder.setContent(new byte[0]);
        Message msg = builder.build();
        assertEquals(0L, msg.getEventTime());
    }

}
