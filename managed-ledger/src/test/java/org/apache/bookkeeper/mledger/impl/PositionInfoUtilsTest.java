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

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.util.Map;
import java.util.List;

import org.apache.bookkeeper.mledger.proto.LightMLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class PositionInfoUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(PositionInfoUtilsTest.class);

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

    @Test
    public void testSerializeDeserialize2() throws Exception {
        PositionImpl position = new PositionImpl(1, 2);
        ManagedCursorImpl.MarkDeleteEntry entry = new ManagedCursorImpl.MarkDeleteEntry(position,
                Map.of("foo", 1L), null, null);

        final int numRanges = 10000;
        ByteBuf result = PositionInfoUtils.serializePositionInfo(entry, position, (scanner) -> {
            for (int i = 0; i < numRanges; i++) {
                scanner.acceptRange(i*4 + 1, i*4 + 2, i*4 + 3, i*4 + 4);
            }
        }, (scanner) -> {
            long[] array = {7L, 8L};
            for (int i = 0; i < numRanges; i++) {
                scanner.acceptRange(i*2 + 1, i*2 + 2, array);
            }
        }, 1024);

        // deserialize PIUtils -> lightproto
        final int idx = result.readerIndex();
        LightMLDataFormats.PositionInfo lighPositionInfoParsed = new LightMLDataFormats.PositionInfo();
        lighPositionInfoParsed.parseFrom(result, result.readableBytes());
        result.readerIndex(idx);

        validateLightproto(lighPositionInfoParsed, numRanges);

        // serialize lightproto
        int serializedSz = lighPositionInfoParsed.getSerializedSize();
        ByteBuf lightResult = PulsarByteBufAllocator.DEFAULT.buffer(serializedSz);
        lighPositionInfoParsed.writeTo(lightResult);

        byte[] light = ByteBufUtil.getBytes(lightResult);
        byte[] util = ByteBufUtil.getBytes(result);

        assertEquals(light.length, util.length);

        for (int i = 0; i < light.length; i++) {
            if (light[i] != util[i]) {
                log.error("Mismatch at index {} light={} util={}", i, light[i], util[i]);
            }
        }

        assertEquals(light, util);

        // deserialize lightproto -> protobuf
        parseProtobufAndValidate(light, numRanges);

        // deserialize PIUtils -> protobuf
        parseProtobufAndValidate(util, numRanges);

        result.release();
        lightResult.release();
    }

    private static void validateLightproto(LightMLDataFormats.PositionInfo lighPositionInfoParsed, int numRanges) {
        assertEquals(1, lighPositionInfoParsed.getLedgerId());
        assertEquals(2, lighPositionInfoParsed.getEntryId());

        assertEquals(1, lighPositionInfoParsed.getPropertiesCount());
        assertEquals("foo", lighPositionInfoParsed.getPropertyAt(0).getName());
        assertEquals(1, lighPositionInfoParsed.getPropertyAt(0).getValue());

        assertEquals(numRanges, lighPositionInfoParsed.getIndividualDeletedMessagesCount());
        int curr = 0;
        for (int i = 0; i < numRanges; i++) {
            assertEquals(i*4 + 1, lighPositionInfoParsed.getIndividualDeletedMessageAt(curr).getLowerEndpoint().getLedgerId());
            assertEquals(i*4 + 2, lighPositionInfoParsed.getIndividualDeletedMessageAt(curr).getLowerEndpoint().getEntryId());
            assertEquals(i*4 + 3, lighPositionInfoParsed.getIndividualDeletedMessageAt(curr).getUpperEndpoint().getLedgerId());
            assertEquals(i*4 + 4, lighPositionInfoParsed.getIndividualDeletedMessageAt(curr).getUpperEndpoint().getEntryId());
            curr++;
        }

        assertEquals(numRanges, lighPositionInfoParsed.getBatchedEntryDeletionIndexInfosCount());
        curr = 0;
        for (int i = 0; i < numRanges; i++) {
            assertEquals(i*2 + 1, lighPositionInfoParsed.getBatchedEntryDeletionIndexInfoAt(curr).getPosition().getLedgerId());
            assertEquals(i*2 + 2, lighPositionInfoParsed.getBatchedEntryDeletionIndexInfoAt(curr).getPosition().getEntryId());
            assertEquals(7L, lighPositionInfoParsed.getBatchedEntryDeletionIndexInfoAt(curr).getDeleteSetAt(0));
            assertEquals(8L, lighPositionInfoParsed.getBatchedEntryDeletionIndexInfoAt(curr).getDeleteSetAt(1));
            curr++;
        }
    }

    private static void parseProtobufAndValidate(byte[] data, int numRanges) throws InvalidProtocolBufferException {
        MLDataFormats.PositionInfo positionInfoParsed = MLDataFormats.PositionInfo.parseFrom(data);

        assertEquals(1, positionInfoParsed.getLedgerId());
        assertEquals(2, positionInfoParsed.getEntryId());

        assertEquals(1, positionInfoParsed.getPropertiesCount());
        assertEquals("foo", positionInfoParsed.getProperties(0).getName());
        assertEquals(1, positionInfoParsed.getProperties(0).getValue());

        assertEquals(numRanges, positionInfoParsed.getIndividualDeletedMessagesCount());
        int curr = 0;
        for (int i = 0; i < numRanges; i++) {
            assertEquals(i*4 + 1, positionInfoParsed.getIndividualDeletedMessages(curr).getLowerEndpoint().getLedgerId());
            assertEquals(i*4 + 2, positionInfoParsed.getIndividualDeletedMessages(curr).getLowerEndpoint().getEntryId());
            assertEquals(i*4 + 3, positionInfoParsed.getIndividualDeletedMessages(curr).getUpperEndpoint().getLedgerId());
            assertEquals(i*4 + 4, positionInfoParsed.getIndividualDeletedMessages(curr).getUpperEndpoint().getEntryId());
            curr++;
        }

        assertEquals(numRanges, positionInfoParsed.getBatchedEntryDeletionIndexInfoCount());
        curr = 0;
        for (int i = 0; i < numRanges; i++) {
            assertEquals(i*2 + 1, positionInfoParsed.getBatchedEntryDeletionIndexInfo(curr).getPosition().getLedgerId());
            assertEquals(i*2 + 2, positionInfoParsed.getBatchedEntryDeletionIndexInfo(curr).getPosition().getEntryId());
            assertEquals(List.of(7L, 8L), positionInfoParsed.getBatchedEntryDeletionIndexInfo(curr).getDeleteSetList());
            curr++;
        }
    }

}