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

import static org.testng.Assert.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.util.Map;
import java.util.List;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.testng.annotations.Test;

public class PositionInfoUtilsTest {
    @Test
    public void testSerializeDeserialize() throws Exception {
        PositionImpl position = new PositionImpl(1, 2);
        ManagedCursorImpl.MarkDeleteEntry entry = new ManagedCursorImpl.MarkDeleteEntry(position,
                Map.of("foo", 1L), null, null);

        ByteBuf result = PositionInfoUtils.serializePositionInfo(entry, position, (scanner) -> {
            scanner.acceptRange(1, 2, 3, 4);
            scanner.acceptRange(5, 6, 7, 8);
        }, (scanner) -> {
            long[] array = {7L, 8L};
            scanner.acceptRange(1, 2, array);
        }, 1024);

        byte[] data = ByteBufUtil.getBytes(result);
        MLDataFormats.PositionInfo positionInfoParsed = MLDataFormats.PositionInfo.parseFrom(data);
        assertEquals(1, positionInfoParsed.getLedgerId());
        assertEquals(2, positionInfoParsed.getEntryId());

        assertEquals(1, positionInfoParsed.getPropertiesCount());
        assertEquals("foo", positionInfoParsed.getProperties(0).getName());
        assertEquals(1, positionInfoParsed.getProperties(0).getValue());

        assertEquals(2, positionInfoParsed.getIndividualDeletedMessagesCount());
        assertEquals(1, positionInfoParsed.getIndividualDeletedMessages(0).getLowerEndpoint().getLedgerId());
        assertEquals(2, positionInfoParsed.getIndividualDeletedMessages(0).getLowerEndpoint().getEntryId());
        assertEquals(3, positionInfoParsed.getIndividualDeletedMessages(0).getUpperEndpoint().getLedgerId());
        assertEquals(4, positionInfoParsed.getIndividualDeletedMessages(0).getUpperEndpoint().getEntryId());

        assertEquals(5, positionInfoParsed.getIndividualDeletedMessages(1).getLowerEndpoint().getLedgerId());
        assertEquals(6, positionInfoParsed.getIndividualDeletedMessages(1).getLowerEndpoint().getEntryId());
        assertEquals(7, positionInfoParsed.getIndividualDeletedMessages(1).getUpperEndpoint().getLedgerId());
        assertEquals(8, positionInfoParsed.getIndividualDeletedMessages(1).getUpperEndpoint().getEntryId());

        assertEquals(1, positionInfoParsed.getBatchedEntryDeletionIndexInfoCount());
        assertEquals(1, positionInfoParsed.getBatchedEntryDeletionIndexInfo(0).getPosition().getLedgerId());
        assertEquals(2, positionInfoParsed.getBatchedEntryDeletionIndexInfo(0).getPosition().getEntryId());
        assertEquals(List.of(7L, 8L), positionInfoParsed.getBatchedEntryDeletionIndexInfo(0).getDeleteSetList());
        result.release();
    }

    @Test
    public void testSerializeDeserializeEmpty() throws Exception {
        PositionImpl position = new PositionImpl(1, 2);
        ManagedCursorImpl.MarkDeleteEntry entry = new ManagedCursorImpl.MarkDeleteEntry(position,
               null, null, null);

        ByteBuf result = PositionInfoUtils.serializePositionInfo(entry, position, (scanner) -> {
        }, (scanner) -> {
        }, 1024);

        byte[] data = ByteBufUtil.getBytes(result);
        MLDataFormats.PositionInfo positionInfoParsed = MLDataFormats.PositionInfo.parseFrom(data);
        assertEquals(1, positionInfoParsed.getLedgerId());
        assertEquals(2, positionInfoParsed.getEntryId());

        assertEquals(0, positionInfoParsed.getPropertiesCount());
        assertEquals(0, positionInfoParsed.getIndividualDeletedMessagesCount());
        assertEquals(0, positionInfoParsed.getBatchedEntryDeletionIndexInfoCount());
        result.release();
    }
}