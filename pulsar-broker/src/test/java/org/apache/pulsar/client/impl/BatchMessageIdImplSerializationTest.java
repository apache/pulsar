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

import static org.testng.Assert.assertEquals;

import java.io.IOException;

import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class BatchMessageIdImplSerializationTest {

    @Test
    public void testSerialization1() throws Exception {
        BatchMessageIdImpl id = new BatchMessageIdImpl(1, 2, 3, 4);
        byte[] serializedId = id.toByteArray();
        assertEquals(BatchMessageIdImpl.fromByteArray(serializedId), id);
    }

    @Test
    public void testSerialization2() throws Exception {
        BatchMessageIdImpl id = new BatchMessageIdImpl(1, 2, -1, 3);
        byte[] serializedId = id.toByteArray();
        assertEquals(BatchMessageIdImpl.fromByteArray(serializedId), id);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSerializationNull() throws Exception {
        BatchMessageIdImpl.fromByteArray(null);
    }

    @Test(expectedExceptions = IOException.class)
    public void testSerializationEmpty() throws Exception {
        BatchMessageIdImpl.fromByteArray(new byte[0]);
    }
}
