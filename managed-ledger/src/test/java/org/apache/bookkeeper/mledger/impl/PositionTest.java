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
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.testng.annotations.Test;

public class PositionTest {
    @Test(expectedExceptions = NullPointerException.class)
    public void nullParam() {
        new PositionImpl((PositionInfo) null);
    }

    @Test
    public void simpleTest() {
        PositionImpl pos = new PositionImpl(1, 2);
        assertEquals(pos.getLedgerId(), 1);
        assertEquals(pos.getEntryId(), 2);
        assertEquals(pos, new PositionImpl(1, 2));

        assertNotEquals(new PositionImpl(1, 3), pos);
        assertNotEquals(new PositionImpl(3, 2), pos);
        assertNotEquals(pos, "1:2");
    }

    @Test
    public void comparisons() {
        PositionImpl pos1_1 = new PositionImpl(1, 1);
        PositionImpl pos2_5 = new PositionImpl(2, 5);
        PositionImpl pos10_0 = new PositionImpl(10, 0);
        PositionImpl pos10_1 = new PositionImpl(10, 1);

        assertEquals(0, pos1_1.compareTo(pos1_1));
        assertEquals(-1, pos1_1.compareTo(pos2_5));
        assertEquals(-1, pos1_1.compareTo(pos10_0));
        assertEquals(-1, pos1_1.compareTo(pos10_1));

        assertEquals(+1, pos2_5.compareTo(pos1_1));
        assertEquals(0, pos2_5.compareTo(pos2_5));
        assertEquals(-1, pos2_5.compareTo(pos10_0));
        assertEquals(-1, pos2_5.compareTo(pos10_1));

        assertEquals(+1, pos10_0.compareTo(pos1_1));
        assertEquals(+1, pos10_0.compareTo(pos2_5));
        assertEquals(0, pos10_0.compareTo(pos10_0));
        assertEquals(-1, pos10_0.compareTo(pos10_1));

        assertEquals(+1, pos10_1.compareTo(pos1_1));
        assertEquals(+1, pos10_1.compareTo(pos2_5));
        assertEquals(+1, pos10_1.compareTo(pos10_0));
        assertEquals(0, pos10_1.compareTo(pos10_1));
    }

    @Test
    public void hashes() throws Exception {
        PositionImpl p1 = new PositionImpl(5, 15);
        PositionImpl p2 = new PositionImpl(PositionInfo.parseFrom(p1.getPositionInfo().toByteArray()));
        assertEquals(p2.getLedgerId(), 5);
        assertEquals(p2.getEntryId(), 15);
        assertEquals(new PositionImpl(5, 15).hashCode(), p2.hashCode());
    }
}
