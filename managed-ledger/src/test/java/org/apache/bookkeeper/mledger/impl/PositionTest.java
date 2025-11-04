/*
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
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.PositionInfo;
import org.testng.annotations.Test;

public class PositionTest {
    @Test(expectedExceptions = NullPointerException.class)
    public void nullParam() {
        PositionFactory.create(null);
    }

    @Test
    public void simpleTest() {
        Position pos = PositionFactory.create(1, 2);
        assertEquals(pos.getLedgerId(), 1);
        assertEquals(pos.getEntryId(), 2);
        assertEquals(pos, PositionFactory.create(1, 2));

        assertNotEquals(PositionFactory.create(1, 3), pos);
        assertNotEquals(PositionFactory.create(3, 2), pos);
        assertNotEquals(pos, "1:2");
    }

    @Test
    public void comparisons() {
        Position pos11 = PositionFactory.create(1, 1);
        Position pos25 = PositionFactory.create(2, 5);
        Position pos100 = PositionFactory.create(10, 0);
        Position pos101 = PositionFactory.create(10, 1);

        assertEquals(0, pos11.compareTo(pos11));
        assertEquals(-1, pos11.compareTo(pos25));
        assertEquals(-1, pos11.compareTo(pos100));
        assertEquals(-1, pos11.compareTo(pos101));

        assertEquals(+1, pos25.compareTo(pos11));
        assertEquals(0, pos25.compareTo(pos25));
        assertEquals(-1, pos25.compareTo(pos100));
        assertEquals(-1, pos25.compareTo(pos101));

        assertEquals(+1, pos100.compareTo(pos11));
        assertEquals(+1, pos100.compareTo(pos25));
        assertEquals(0, pos100.compareTo(pos100));
        assertEquals(-1, pos100.compareTo(pos101));

        assertEquals(+1, pos101.compareTo(pos11));
        assertEquals(+1, pos101.compareTo(pos25));
        assertEquals(+1, pos101.compareTo(pos100));
        assertEquals(0, pos101.compareTo(pos101));
    }

    @Test
    public void hashes() throws Exception {
        Position p1 = PositionFactory.create(5, 15);
        PositionInfo positionInfo =
                PositionInfo.newBuilder().setLedgerId(p1.getLedgerId()).setEntryId(p1.getEntryId()).build();
        PositionInfo parsed = PositionInfo.parseFrom(positionInfo.toByteArray());
        Position p2 = PositionFactory.create(parsed.getLedgerId(), parsed.getEntryId());
        assertEquals(p2.getLedgerId(), 5);
        assertEquals(p2.getEntryId(), 15);
        assertEquals(PositionFactory.create(5, 15).hashCode(), p2.hashCode());
    }
}
