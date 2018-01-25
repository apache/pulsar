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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.util.SeekPosition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SeekPositionTest {
    @Test
    void TestSeekPositionUtil() {
        MessageId earlist = MessageIdImpl.earliest;
        MessageId latest = MessageIdImpl.latest;
        MessageId defaul = SeekPosition.getPosition(SeekPosition.seekPositionDefault);
        Assert.assertEquals(SeekPosition.getPosition(SeekPosition.seekPositionEarliest), earlist);
        Assert.assertEquals(SeekPosition.getPosition(SeekPosition.seekPositionLatest), latest);
        Assert.assertEquals(SeekPosition.getPosition(null), defaul);
        Assert.assertEquals(SeekPosition.getPosition("123"), defaul);
        Assert.assertEquals(SeekPosition.getPosition("hello"), defaul);   
    }
}