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

package org.apache.pulsar.functions.fs;

import static org.testng.Assert.assertEquals;

import org.apache.pulsar.functions.fs.FunctionID;
import org.testng.annotations.Test;

/**
 * Unit test of {@link FunctionID}.
 */
public class FunctionIDTest {

    @Test
    public void testToString() {
        FunctionID fid = new FunctionID();
        assertEquals(
            "fn-" + fid.getInternalUUID(),
            fid.toString());
    }

    @Test
    public void testConstructor() {
        FunctionID fid = new FunctionID(1234L, 3456L);
        assertEquals(
            "fn-" + new java.util.UUID(1234L, 3456L).toString(),
            fid.toString());
        assertEquals(1234L, fid.getInternalUUID().getMostSignificantBits());
        assertEquals(3456L, fid.getInternalUUID().getLeastSignificantBits());
    }

}
