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

import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BatchMessageIdImplTest {

    @Test
    public void compareToTest() {
        BatchMessageIdImpl batchMsgId1 = new BatchMessageIdImpl(0, 0, 0, 0);
        BatchMessageIdImpl batchMsgId2 = new BatchMessageIdImpl(1, 1, 1, 1);

        Assert.assertEquals(batchMsgId1.compareTo(batchMsgId2), -1);
        Assert.assertEquals(batchMsgId2.compareTo(batchMsgId1), 1);
        Assert.assertEquals(batchMsgId2.compareTo(batchMsgId2), 0);

    }

    @Test
    public void hashCodeTest() {
        BatchMessageIdImpl batchMsgId1 = new BatchMessageIdImpl(0, 0, 0, 0);
        BatchMessageIdImpl batchMsgId2 = new BatchMessageIdImpl(1, 1, 1, 1);

        Assert.assertEquals(batchMsgId1.hashCode(), batchMsgId1.hashCode());
        Assert.assertTrue(batchMsgId1.hashCode() != batchMsgId2.hashCode());

    }

    @Test
    public void equalsTest() {
        BatchMessageIdImpl batchMsgId1 = new BatchMessageIdImpl(0, 0, 0, 0);
        BatchMessageIdImpl batchMsgId2 = new BatchMessageIdImpl(1, 1, 1, 1);
        BatchMessageIdImpl batchMsgId3 = new BatchMessageIdImpl(0, 0, 0, 1);
        BatchMessageIdImpl batchMsgId4 = new BatchMessageIdImpl(0, 0, 0, -1);
        MessageIdImpl msgId = new MessageIdImpl(0, 0, 0);

        Assert.assertTrue(batchMsgId1.equals(batchMsgId1));
        Assert.assertFalse(batchMsgId1.equals(batchMsgId2));
        Assert.assertFalse(batchMsgId1.equals(batchMsgId3));
        Assert.assertFalse(batchMsgId1.equals(batchMsgId4));
        Assert.assertFalse(batchMsgId1.equals(msgId));

        Assert.assertTrue(msgId.equals(msgId));
        Assert.assertFalse(msgId.equals(batchMsgId1));
        Assert.assertFalse(msgId.equals(batchMsgId2));
        Assert.assertFalse(msgId.equals(batchMsgId3));
        Assert.assertTrue(msgId.equals(batchMsgId4));

        Assert.assertTrue(batchMsgId4.equals(msgId));
    }

}
