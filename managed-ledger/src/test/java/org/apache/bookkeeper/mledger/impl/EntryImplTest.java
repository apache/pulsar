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

import static org.testng.Assert.assertTrue;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.Test;

public class EntryImplTest {

    @Test
    public void testRecycledObjectDoesNotInheritPoisonedPosition() {
        // Given a legitimate entry that is released normally
        EntryImpl first = EntryImpl.create(5L, 10L, new byte[]{1, 2, 3});
        first.release();

        // When a getPosition() call slips in AFTER the release: deallocation nulls the lazy
        // position field, so this late reader re-materializes it from the reset ids as (-1, -1)
        // and leaves the poisoned value cached inside the pooled object.
        first.getPosition();

        // Then the next create() (the recycler hands back the most recently released object on
        // the same thread) must not report that stale (-1, -1) position as its own — through
        // both the byte[] and the ByteBuf variants, which own the lazy field.
        EntryImpl second = EntryImpl.create(6L, 20L, new byte[]{4, 5, 6});
        assertTrue(second.getPosition().compareTo(PositionFactory.create(6L, 20L)) == 0,
                "byte[] variant: a recycled entry must not inherit the poisoned (-1, -1) position");
        second.release();

        second.getPosition(); // re-poison the recycled object
        EntryImpl third = EntryImpl.create(7L, 30L, Unpooled.wrappedBuffer(new byte[]{7, 8}));
        assertTrue(third.getPosition().compareTo(PositionFactory.create(7L, 30L)) == 0,
                "ByteBuf variant: a recycled entry must not inherit the poisoned (-1, -1) position");
        third.release();
    }
}
