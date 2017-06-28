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
}
