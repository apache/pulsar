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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

final class PositionInfoUtils {

    interface IndividuallyDeletedMessagesRangeConsumer {
        void acceptRange(long lowerLegerId, long lowerEntryId, long upperLedgerId, long upperEntryId);
    }

    interface BatchedEntryDeletionIndexInfoConsumer {
        void acceptRange(long ledgerId, long entryId, long[] array);
    }

    static ByteBuf serializePositionInfo(ManagedCursorImpl.MarkDeleteEntry mdEntry, PositionImpl position,
                                         Consumer<IndividuallyDeletedMessagesRangeConsumer> rangeScanner,
                                         Consumer<BatchedEntryDeletionIndexInfoConsumer> batchDeletedIndexesScanner,
                                         int lastSerializedSize) {
        int size = Math.max(lastSerializedSize, 64 * 1024);
        ByteBuf _b = PulsarByteBufAllocator.DEFAULT.buffer(size);

        LightProtoCodec.writeVarInt(_b, PositionInfo._LEDGER_ID_TAG);
        LightProtoCodec.writeVarInt64(_b, position.getLedgerId());
        LightProtoCodec.writeVarInt(_b, PositionInfo._ENTRY_ID_TAG);
        LightProtoCodec.writeVarInt64(_b, position.getEntryId());

        MessageRange _item = new MessageRange();
        rangeScanner.accept(new IndividuallyDeletedMessagesRangeConsumer() {
            @Override
            public void acceptRange(long lowerLegerId, long lowerEntryId, long upperLedgerId, long upperEntryId) {
                _item.clear();
                NestedPositionInfo lower = _item.setLowerEndpoint();
                NestedPositionInfo upper = _item.setUpperEndpoint();
                lower.setLedgerId(lowerLegerId);
                lower.setEntryId(lowerEntryId);
                upper.setLedgerId(upperLedgerId);
                upper.setEntryId(upperEntryId);
                LightProtoCodec.writeVarInt(_b, PositionInfo._INDIVIDUAL_DELETED_MESSAGES_TAG);
                LightProtoCodec.writeVarInt(_b, _item.getSerializedSize());
                _item.writeTo(_b);
            }
        });

        final LongProperty longProperty = new LongProperty();
        Map<String, Long> properties = mdEntry.properties;
        if (properties != null) {
            properties.forEach((k, v) -> {
                longProperty.setName(k);
                longProperty.setValue(v);
                LightProtoCodec.writeVarInt(_b, PositionInfo._PROPERTIES_TAG);
                LightProtoCodec.writeVarInt(_b, longProperty.getSerializedSize());
                longProperty.writeTo(_b);
            });
        }

        final BatchedEntryDeletionIndexInfo batchDeletedIndexInfo = new BatchedEntryDeletionIndexInfo();

        batchDeletedIndexesScanner.accept(new BatchedEntryDeletionIndexInfoConsumer() {
            @Override
            public void acceptRange(long ledgerId, long entryId, long[] array) {
                batchDeletedIndexInfo.clear();
                final NestedPositionInfo nestedPositionInfo = batchDeletedIndexInfo.setPosition();
                nestedPositionInfo.setLedgerId(ledgerId);
                nestedPositionInfo.setEntryId(entryId);
                for (long l : array) {
                    batchDeletedIndexInfo.addDeleteSet(l);
                }
                LightProtoCodec.writeVarInt(_b, PositionInfo._BATCHED_ENTRY_DELETION_INDEX_INFO_TAG);
                LightProtoCodec.writeVarInt(_b, batchDeletedIndexInfo.getSerializedSize());
                batchDeletedIndexInfo.writeTo(_b);
            }
        });

        return _b;
    }

    public static final class PositionInfo {
        private long ledgerId;
        private static final int _LEDGER_ID_FIELD_NUMBER = 1;
        private static final int _LEDGER_ID_TAG = (_LEDGER_ID_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_VARINT;
        private static final int _LEDGER_ID_TAG_SIZE = LightProtoCodec.computeVarIntSize(_LEDGER_ID_TAG);
        private static final int _LEDGER_ID_MASK = 1 << (0 % 32);
        public boolean hasLedgerId() {
            return (_bitField0 & _LEDGER_ID_MASK) != 0;
        }
        public long getLedgerId() {
            if (!hasLedgerId()) {
                throw new IllegalStateException("Field 'ledgerId' is not set");
            }
            return ledgerId;
        }
        public PositionInfo setLedgerId(long ledgerId) {
            this.ledgerId = ledgerId;
            _bitField0 |= _LEDGER_ID_MASK;
            _cachedSize = -1;
            return this;
        }
        public PositionInfo clearLedgerId() {
            _bitField0 &= ~_LEDGER_ID_MASK;
            return this;
        }

        private long entryId;
        private static final int _ENTRY_ID_FIELD_NUMBER = 2;
        private static final int _ENTRY_ID_TAG = (_ENTRY_ID_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_VARINT;
        private static final int _ENTRY_ID_TAG_SIZE = LightProtoCodec.computeVarIntSize(_ENTRY_ID_TAG);
        private static final int _ENTRY_ID_MASK = 1 << (1 % 32);
        public boolean hasEntryId() {
            return (_bitField0 & _ENTRY_ID_MASK) != 0;
        }
        public long getEntryId() {
            if (!hasEntryId()) {
                throw new IllegalStateException("Field 'entryId' is not set");
            }
            return entryId;
        }
        public PositionInfo setEntryId(long entryId) {
            this.entryId = entryId;
            _bitField0 |= _ENTRY_ID_MASK;
            _cachedSize = -1;
            return this;
        }
        public PositionInfo clearEntryId() {
            _bitField0 &= ~_ENTRY_ID_MASK;
            return this;
        }

        private java.util.List<MessageRange> individualDeletedMessages = null;
        private int _individualDeletedMessagesCount = 0;
        private static final int _INDIVIDUAL_DELETED_MESSAGES_FIELD_NUMBER = 3;
        private static final int _INDIVIDUAL_DELETED_MESSAGES_TAG = (_INDIVIDUAL_DELETED_MESSAGES_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _INDIVIDUAL_DELETED_MESSAGES_TAG_SIZE = LightProtoCodec
                .computeVarIntSize(_INDIVIDUAL_DELETED_MESSAGES_TAG);
        public int getIndividualDeletedMessagesCount() {
            return _individualDeletedMessagesCount;
        }
        public MessageRange getIndividualDeletedMessageAt(int idx) {
            if (idx < 0 || idx >= _individualDeletedMessagesCount) {
                throw new IndexOutOfBoundsException("Index " + idx + " is out of the list size ("
                        + _individualDeletedMessagesCount + ") for field 'individualDeletedMessages'");
            }
            return individualDeletedMessages.get(idx);
        }
        public java.util.List<MessageRange> getIndividualDeletedMessagesList() {
            if (_individualDeletedMessagesCount == 0) {
                return java.util.Collections.emptyList();
            } else {
                return individualDeletedMessages.subList(0, _individualDeletedMessagesCount);
            }
        }
        public MessageRange addIndividualDeletedMessage() {
            if (individualDeletedMessages == null) {
                individualDeletedMessages = new java.util.ArrayList<MessageRange>();
            }
            if (individualDeletedMessages.size() == _individualDeletedMessagesCount) {
                individualDeletedMessages.add(new MessageRange());
            }
            _cachedSize = -1;
            return individualDeletedMessages.get(_individualDeletedMessagesCount++);
        }
        public PositionInfo addAllIndividualDeletedMessages(Iterable<MessageRange> individualDeletedMessages) {
            for (MessageRange _o : individualDeletedMessages) {
                addIndividualDeletedMessage().copyFrom(_o);
            }
            return this;
        }
        public PositionInfo clearIndividualDeletedMessages() {
            for (int i = 0; i < _individualDeletedMessagesCount; i++) {
                individualDeletedMessages.get(i).clear();
            }
            _individualDeletedMessagesCount = 0;
            return this;
        }

        private java.util.List<LongProperty> properties = null;
        private int _propertiesCount = 0;
        private static final int _PROPERTIES_FIELD_NUMBER = 4;
        private static final int _PROPERTIES_TAG = (_PROPERTIES_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _PROPERTIES_TAG_SIZE = LightProtoCodec.computeVarIntSize(_PROPERTIES_TAG);
        public int getPropertiesCount() {
            return _propertiesCount;
        }
        public LongProperty getPropertyAt(int idx) {
            if (idx < 0 || idx >= _propertiesCount) {
                throw new IndexOutOfBoundsException(
                        "Index " + idx + " is out of the list size (" + _propertiesCount + ") for field 'properties'");
            }
            return properties.get(idx);
        }
        public java.util.List<LongProperty> getPropertiesList() {
            if (_propertiesCount == 0) {
                return java.util.Collections.emptyList();
            } else {
                return properties.subList(0, _propertiesCount);
            }
        }
        public LongProperty addProperty() {
            if (properties == null) {
                properties = new java.util.ArrayList<LongProperty>();
            }
            if (properties.size() == _propertiesCount) {
                properties.add(new LongProperty());
            }
            _cachedSize = -1;
            return properties.get(_propertiesCount++);
        }
        public PositionInfo addAllProperties(Iterable<LongProperty> properties) {
            for (LongProperty _o : properties) {
                addProperty().copyFrom(_o);
            }
            return this;
        }
        public PositionInfo clearProperties() {
            for (int i = 0; i < _propertiesCount; i++) {
                properties.get(i).clear();
            }
            _propertiesCount = 0;
            return this;
        }

        private java.util.List<BatchedEntryDeletionIndexInfo> batchedEntryDeletionIndexInfos = null;
        private int _batchedEntryDeletionIndexInfosCount = 0;
        private static final int _BATCHED_ENTRY_DELETION_INDEX_INFO_FIELD_NUMBER = 5;
        private static final int _BATCHED_ENTRY_DELETION_INDEX_INFO_TAG = (_BATCHED_ENTRY_DELETION_INDEX_INFO_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _BATCHED_ENTRY_DELETION_INDEX_INFO_TAG_SIZE = LightProtoCodec
                .computeVarIntSize(_BATCHED_ENTRY_DELETION_INDEX_INFO_TAG);
        public int getBatchedEntryDeletionIndexInfosCount() {
            return _batchedEntryDeletionIndexInfosCount;
        }
        public BatchedEntryDeletionIndexInfo getBatchedEntryDeletionIndexInfoAt(int idx) {
            if (idx < 0 || idx >= _batchedEntryDeletionIndexInfosCount) {
                throw new IndexOutOfBoundsException("Index " + idx + " is out of the list size ("
                        + _batchedEntryDeletionIndexInfosCount + ") for field 'batchedEntryDeletionIndexInfo'");
            }
            return batchedEntryDeletionIndexInfos.get(idx);
        }
        public java.util.List<BatchedEntryDeletionIndexInfo> getBatchedEntryDeletionIndexInfosList() {
            if (_batchedEntryDeletionIndexInfosCount == 0) {
                return java.util.Collections.emptyList();
            } else {
                return batchedEntryDeletionIndexInfos.subList(0, _batchedEntryDeletionIndexInfosCount);
            }
        }
        public BatchedEntryDeletionIndexInfo addBatchedEntryDeletionIndexInfo() {
            if (batchedEntryDeletionIndexInfos == null) {
                batchedEntryDeletionIndexInfos = new java.util.ArrayList<BatchedEntryDeletionIndexInfo>();
            }
            if (batchedEntryDeletionIndexInfos.size() == _batchedEntryDeletionIndexInfosCount) {
                batchedEntryDeletionIndexInfos.add(new BatchedEntryDeletionIndexInfo());
            }
            _cachedSize = -1;
            return batchedEntryDeletionIndexInfos.get(_batchedEntryDeletionIndexInfosCount++);
        }
        public PositionInfo addAllBatchedEntryDeletionIndexInfos(
                Iterable<BatchedEntryDeletionIndexInfo> batchedEntryDeletionIndexInfos) {
            for (BatchedEntryDeletionIndexInfo _o : batchedEntryDeletionIndexInfos) {
                addBatchedEntryDeletionIndexInfo().copyFrom(_o);
            }
            return this;
        }
        public PositionInfo clearBatchedEntryDeletionIndexInfo() {
            for (int i = 0; i < _batchedEntryDeletionIndexInfosCount; i++) {
                batchedEntryDeletionIndexInfos.get(i).clear();
            }
            _batchedEntryDeletionIndexInfosCount = 0;
            return this;
        }

        private int _bitField0;
        private static final int _REQUIRED_FIELDS_MASK0 = 0 | _LEDGER_ID_MASK | _ENTRY_ID_MASK;
        public int writeTo(io.netty.buffer.ByteBuf _b) {
            checkRequiredFields();
            int _writeIdx = _b.writerIndex();
            LightProtoCodec.writeVarInt(_b, _LEDGER_ID_TAG);
            LightProtoCodec.writeVarInt64(_b, ledgerId);
            LightProtoCodec.writeVarInt(_b, _ENTRY_ID_TAG);
            LightProtoCodec.writeVarInt64(_b, entryId);
            for (int i = 0; i < _individualDeletedMessagesCount; i++) {
                MessageRange _item = individualDeletedMessages.get(i);
                LightProtoCodec.writeVarInt(_b, _INDIVIDUAL_DELETED_MESSAGES_TAG);
                LightProtoCodec.writeVarInt(_b, _item.getSerializedSize());
                _item.writeTo(_b);
            }
            for (int i = 0; i < _propertiesCount; i++) {
                LongProperty _item = properties.get(i);
                LightProtoCodec.writeVarInt(_b, _PROPERTIES_TAG);
                LightProtoCodec.writeVarInt(_b, _item.getSerializedSize());
                _item.writeTo(_b);
            }
            for (int i = 0; i < _batchedEntryDeletionIndexInfosCount; i++) {
                BatchedEntryDeletionIndexInfo _item = batchedEntryDeletionIndexInfos.get(i);
                LightProtoCodec.writeVarInt(_b, _BATCHED_ENTRY_DELETION_INDEX_INFO_TAG);
                LightProtoCodec.writeVarInt(_b, _item.getSerializedSize());
                _item.writeTo(_b);
            }
            return (_b.writerIndex() - _writeIdx);
        }
        public int getSerializedSize() {
            if (_cachedSize > -1) {
                return _cachedSize;
            }

            int _size = 0;
            _size += _LEDGER_ID_TAG_SIZE;
            _size += LightProtoCodec.computeVarInt64Size(ledgerId);
            _size += _ENTRY_ID_TAG_SIZE;
            _size += LightProtoCodec.computeVarInt64Size(entryId);
            for (int i = 0; i < _individualDeletedMessagesCount; i++) {
                MessageRange _item = individualDeletedMessages.get(i);
                _size += _INDIVIDUAL_DELETED_MESSAGES_TAG_SIZE;
                int MsgsizeIndividualDeletedMessages = _item.getSerializedSize();
                _size += LightProtoCodec.computeVarIntSize(MsgsizeIndividualDeletedMessages)
                        + MsgsizeIndividualDeletedMessages;
            }
            for (int i = 0; i < _propertiesCount; i++) {
                LongProperty _item = properties.get(i);
                _size += _PROPERTIES_TAG_SIZE;
                int MsgsizeProperties = _item.getSerializedSize();
                _size += LightProtoCodec.computeVarIntSize(MsgsizeProperties) + MsgsizeProperties;
            }
            for (int i = 0; i < _batchedEntryDeletionIndexInfosCount; i++) {
                BatchedEntryDeletionIndexInfo _item = batchedEntryDeletionIndexInfos.get(i);
                _size += _BATCHED_ENTRY_DELETION_INDEX_INFO_TAG_SIZE;
                int MsgsizeBatchedEntryDeletionIndexInfo = _item.getSerializedSize();
                _size += LightProtoCodec.computeVarIntSize(MsgsizeBatchedEntryDeletionIndexInfo)
                        + MsgsizeBatchedEntryDeletionIndexInfo;
            }
            _cachedSize = _size;
            return _size;
        }
        public void parseFrom(io.netty.buffer.ByteBuf _buffer, int _size) {
            clear();
            int _endIdx = _buffer.readerIndex() + _size;
            while (_buffer.readerIndex() < _endIdx) {
                int _tag = LightProtoCodec.readVarInt(_buffer);
                switch (_tag) {
                    case _LEDGER_ID_TAG :
                        _bitField0 |= _LEDGER_ID_MASK;
                        ledgerId = LightProtoCodec.readVarInt64(_buffer);
                        break;
                    case _ENTRY_ID_TAG :
                        _bitField0 |= _ENTRY_ID_MASK;
                        entryId = LightProtoCodec.readVarInt64(_buffer);
                        break;
                    case _INDIVIDUAL_DELETED_MESSAGES_TAG :
                        int _individualDeletedMessagesSize = LightProtoCodec.readVarInt(_buffer);
                        addIndividualDeletedMessage().parseFrom(_buffer, _individualDeletedMessagesSize);
                        break;
                    case _PROPERTIES_TAG :
                        int _propertiesSize = LightProtoCodec.readVarInt(_buffer);
                        addProperty().parseFrom(_buffer, _propertiesSize);
                        break;
                    case _BATCHED_ENTRY_DELETION_INDEX_INFO_TAG :
                        int _batchedEntryDeletionIndexInfoSize = LightProtoCodec.readVarInt(_buffer);
                        addBatchedEntryDeletionIndexInfo().parseFrom(_buffer, _batchedEntryDeletionIndexInfoSize);
                        break;
                    default :
                        LightProtoCodec.skipUnknownField(_tag, _buffer);
                }
            }
            checkRequiredFields();
            _parsedBuffer = _buffer;
        }
        private void checkRequiredFields() {
            if ((_bitField0 & _REQUIRED_FIELDS_MASK0) != _REQUIRED_FIELDS_MASK0) {
                throw new IllegalStateException("Some required fields are missing");
            }
        }
        public PositionInfo clear() {
            for (int i = 0; i < _individualDeletedMessagesCount; i++) {
                individualDeletedMessages.get(i).clear();
            }
            _individualDeletedMessagesCount = 0;
            for (int i = 0; i < _propertiesCount; i++) {
                properties.get(i).clear();
            }
            _propertiesCount = 0;
            for (int i = 0; i < _batchedEntryDeletionIndexInfosCount; i++) {
                batchedEntryDeletionIndexInfos.get(i).clear();
            }
            _batchedEntryDeletionIndexInfosCount = 0;
            _parsedBuffer = null;
            _cachedSize = -1;
            _bitField0 = 0;
            return this;
        }
        public PositionInfo copyFrom(PositionInfo _other) {
            _cachedSize = -1;
            if (_other.hasLedgerId()) {
                setLedgerId(_other.ledgerId);
            }
            if (_other.hasEntryId()) {
                setEntryId(_other.entryId);
            }
            for (int i = 0; i < _other.getIndividualDeletedMessagesCount(); i++) {
                addIndividualDeletedMessage().copyFrom(_other.getIndividualDeletedMessageAt(i));
            }
            for (int i = 0; i < _other.getPropertiesCount(); i++) {
                addProperty().copyFrom(_other.getPropertyAt(i));
            }
            for (int i = 0; i < _other.getBatchedEntryDeletionIndexInfosCount(); i++) {
                addBatchedEntryDeletionIndexInfo().copyFrom(_other.getBatchedEntryDeletionIndexInfoAt(i));
            }
            return this;
        }
        public byte[] toByteArray() {
            byte[] a = new byte[getSerializedSize()];
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a).writerIndex(0);
            this.writeTo(b);
            return a;
        }
        public void parseFrom(byte[] a) {
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a);
            this.parseFrom(b, b.readableBytes());
        }
        private int _cachedSize;

        private io.netty.buffer.ByteBuf _parsedBuffer;

    }

    public static final class NestedPositionInfo {
        private long ledgerId;
        private static final int _LEDGER_ID_FIELD_NUMBER = 1;
        private static final int _LEDGER_ID_TAG = (_LEDGER_ID_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_VARINT;
        private static final int _LEDGER_ID_TAG_SIZE = LightProtoCodec.computeVarIntSize(_LEDGER_ID_TAG);
        private static final int _LEDGER_ID_MASK = 1 << (0 % 32);
        public boolean hasLedgerId() {
            return (_bitField0 & _LEDGER_ID_MASK) != 0;
        }
        public long getLedgerId() {
            if (!hasLedgerId()) {
                throw new IllegalStateException("Field 'ledgerId' is not set");
            }
            return ledgerId;
        }
        public NestedPositionInfo setLedgerId(long ledgerId) {
            this.ledgerId = ledgerId;
            _bitField0 |= _LEDGER_ID_MASK;
            _cachedSize = -1;
            return this;
        }
        public NestedPositionInfo clearLedgerId() {
            _bitField0 &= ~_LEDGER_ID_MASK;
            return this;
        }

        private long entryId;
        private static final int _ENTRY_ID_FIELD_NUMBER = 2;
        private static final int _ENTRY_ID_TAG = (_ENTRY_ID_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_VARINT;
        private static final int _ENTRY_ID_TAG_SIZE = LightProtoCodec.computeVarIntSize(_ENTRY_ID_TAG);
        private static final int _ENTRY_ID_MASK = 1 << (1 % 32);
        public boolean hasEntryId() {
            return (_bitField0 & _ENTRY_ID_MASK) != 0;
        }
        public long getEntryId() {
            if (!hasEntryId()) {
                throw new IllegalStateException("Field 'entryId' is not set");
            }
            return entryId;
        }
        public NestedPositionInfo setEntryId(long entryId) {
            this.entryId = entryId;
            _bitField0 |= _ENTRY_ID_MASK;
            _cachedSize = -1;
            return this;
        }
        public NestedPositionInfo clearEntryId() {
            _bitField0 &= ~_ENTRY_ID_MASK;
            return this;
        }

        private int _bitField0;
        private static final int _REQUIRED_FIELDS_MASK0 = 0 | _LEDGER_ID_MASK | _ENTRY_ID_MASK;
        public int writeTo(io.netty.buffer.ByteBuf _b) {
            checkRequiredFields();
            int _writeIdx = _b.writerIndex();
            LightProtoCodec.writeVarInt(_b, _LEDGER_ID_TAG);
            LightProtoCodec.writeVarInt64(_b, ledgerId);
            LightProtoCodec.writeVarInt(_b, _ENTRY_ID_TAG);
            LightProtoCodec.writeVarInt64(_b, entryId);
            return (_b.writerIndex() - _writeIdx);
        }
        public int getSerializedSize() {
            if (_cachedSize > -1) {
                return _cachedSize;
            }

            int _size = 0;
            _size += _LEDGER_ID_TAG_SIZE;
            _size += LightProtoCodec.computeVarInt64Size(ledgerId);
            _size += _ENTRY_ID_TAG_SIZE;
            _size += LightProtoCodec.computeVarInt64Size(entryId);
            _cachedSize = _size;
            return _size;
        }
        public void parseFrom(io.netty.buffer.ByteBuf _buffer, int _size) {
            clear();
            int _endIdx = _buffer.readerIndex() + _size;
            while (_buffer.readerIndex() < _endIdx) {
                int _tag = LightProtoCodec.readVarInt(_buffer);
                switch (_tag) {
                    case _LEDGER_ID_TAG :
                        _bitField0 |= _LEDGER_ID_MASK;
                        ledgerId = LightProtoCodec.readVarInt64(_buffer);
                        break;
                    case _ENTRY_ID_TAG :
                        _bitField0 |= _ENTRY_ID_MASK;
                        entryId = LightProtoCodec.readVarInt64(_buffer);
                        break;
                    default :
                        LightProtoCodec.skipUnknownField(_tag, _buffer);
                }
            }
            checkRequiredFields();
            _parsedBuffer = _buffer;
        }
        private void checkRequiredFields() {
            if ((_bitField0 & _REQUIRED_FIELDS_MASK0) != _REQUIRED_FIELDS_MASK0) {
                throw new IllegalStateException("Some required fields are missing");
            }
        }
        public NestedPositionInfo clear() {
            _parsedBuffer = null;
            _cachedSize = -1;
            _bitField0 = 0;
            return this;
        }
        public NestedPositionInfo copyFrom(NestedPositionInfo _other) {
            _cachedSize = -1;
            if (_other.hasLedgerId()) {
                setLedgerId(_other.ledgerId);
            }
            if (_other.hasEntryId()) {
                setEntryId(_other.entryId);
            }
            return this;
        }
        public byte[] toByteArray() {
            byte[] a = new byte[getSerializedSize()];
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a).writerIndex(0);
            this.writeTo(b);
            return a;
        }
        public void parseFrom(byte[] a) {
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a);
            this.parseFrom(b, b.readableBytes());
        }
        private int _cachedSize;

        private io.netty.buffer.ByteBuf _parsedBuffer;

    }

    public static final class MessageRange {
        private NestedPositionInfo lowerEndpoint;
        private static final int _LOWER_ENDPOINT_FIELD_NUMBER = 1;
        private static final int _LOWER_ENDPOINT_TAG = (_LOWER_ENDPOINT_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _LOWER_ENDPOINT_TAG_SIZE = LightProtoCodec.computeVarIntSize(_LOWER_ENDPOINT_TAG);
        private static final int _LOWER_ENDPOINT_MASK = 1 << (0 % 32);
        public boolean hasLowerEndpoint() {
            return (_bitField0 & _LOWER_ENDPOINT_MASK) != 0;
        }
        public NestedPositionInfo getLowerEndpoint() {
            if (!hasLowerEndpoint()) {
                throw new IllegalStateException("Field 'lowerEndpoint' is not set");
            }
            return lowerEndpoint;
        }
        public NestedPositionInfo setLowerEndpoint() {
            if (lowerEndpoint == null) {
                lowerEndpoint = new NestedPositionInfo();
            }
            _bitField0 |= _LOWER_ENDPOINT_MASK;
            _cachedSize = -1;
            return lowerEndpoint;
        }
        public MessageRange clearLowerEndpoint() {
            _bitField0 &= ~_LOWER_ENDPOINT_MASK;
            if (hasLowerEndpoint()) {
                lowerEndpoint.clear();
            }
            return this;
        }

        private NestedPositionInfo upperEndpoint;
        private static final int _UPPER_ENDPOINT_FIELD_NUMBER = 2;
        private static final int _UPPER_ENDPOINT_TAG = (_UPPER_ENDPOINT_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _UPPER_ENDPOINT_TAG_SIZE = LightProtoCodec.computeVarIntSize(_UPPER_ENDPOINT_TAG);
        private static final int _UPPER_ENDPOINT_MASK = 1 << (1 % 32);
        public boolean hasUpperEndpoint() {
            return (_bitField0 & _UPPER_ENDPOINT_MASK) != 0;
        }
        public NestedPositionInfo getUpperEndpoint() {
            if (!hasUpperEndpoint()) {
                throw new IllegalStateException("Field 'upperEndpoint' is not set");
            }
            return upperEndpoint;
        }
        public NestedPositionInfo setUpperEndpoint() {
            if (upperEndpoint == null) {
                upperEndpoint = new NestedPositionInfo();
            }
            _bitField0 |= _UPPER_ENDPOINT_MASK;
            _cachedSize = -1;
            return upperEndpoint;
        }
        public MessageRange clearUpperEndpoint() {
            _bitField0 &= ~_UPPER_ENDPOINT_MASK;
            if (hasUpperEndpoint()) {
                upperEndpoint.clear();
            }
            return this;
        }

        private int _bitField0;
        private static final int _REQUIRED_FIELDS_MASK0 = 0 | _LOWER_ENDPOINT_MASK | _UPPER_ENDPOINT_MASK;
        public int writeTo(io.netty.buffer.ByteBuf _b) {
            checkRequiredFields();
            int _writeIdx = _b.writerIndex();
            LightProtoCodec.writeVarInt(_b, _LOWER_ENDPOINT_TAG);
            LightProtoCodec.writeVarInt(_b, lowerEndpoint.getSerializedSize());
            lowerEndpoint.writeTo(_b);
            LightProtoCodec.writeVarInt(_b, _UPPER_ENDPOINT_TAG);
            LightProtoCodec.writeVarInt(_b, upperEndpoint.getSerializedSize());
            upperEndpoint.writeTo(_b);
            return (_b.writerIndex() - _writeIdx);
        }
        public int getSerializedSize() {
            if (_cachedSize > -1) {
                return _cachedSize;
            }

            int _size = 0;
            _size += LightProtoCodec.computeVarIntSize(_LOWER_ENDPOINT_TAG);
            int MsgsizeLowerEndpoint = lowerEndpoint.getSerializedSize();
            _size += LightProtoCodec.computeVarIntSize(MsgsizeLowerEndpoint) + MsgsizeLowerEndpoint;
            _size += LightProtoCodec.computeVarIntSize(_UPPER_ENDPOINT_TAG);
            int MsgsizeUpperEndpoint = upperEndpoint.getSerializedSize();
            _size += LightProtoCodec.computeVarIntSize(MsgsizeUpperEndpoint) + MsgsizeUpperEndpoint;
            _cachedSize = _size;
            return _size;
        }
        public void parseFrom(io.netty.buffer.ByteBuf _buffer, int _size) {
            clear();
            int _endIdx = _buffer.readerIndex() + _size;
            while (_buffer.readerIndex() < _endIdx) {
                int _tag = LightProtoCodec.readVarInt(_buffer);
                switch (_tag) {
                    case _LOWER_ENDPOINT_TAG :
                        _bitField0 |= _LOWER_ENDPOINT_MASK;
                        int lowerEndpointSize = LightProtoCodec.readVarInt(_buffer);
                        setLowerEndpoint().parseFrom(_buffer, lowerEndpointSize);
                        break;
                    case _UPPER_ENDPOINT_TAG :
                        _bitField0 |= _UPPER_ENDPOINT_MASK;
                        int upperEndpointSize = LightProtoCodec.readVarInt(_buffer);
                        setUpperEndpoint().parseFrom(_buffer, upperEndpointSize);
                        break;
                    default :
                        LightProtoCodec.skipUnknownField(_tag, _buffer);
                }
            }
            checkRequiredFields();
            _parsedBuffer = _buffer;
        }
        private void checkRequiredFields() {
            if ((_bitField0 & _REQUIRED_FIELDS_MASK0) != _REQUIRED_FIELDS_MASK0) {
                throw new IllegalStateException("Some required fields are missing");
            }
        }
        public MessageRange clear() {
            if (hasLowerEndpoint()) {
                lowerEndpoint.clear();
            }
            if (hasUpperEndpoint()) {
                upperEndpoint.clear();
            }
            _parsedBuffer = null;
            _cachedSize = -1;
            _bitField0 = 0;
            return this;
        }
        public MessageRange copyFrom(MessageRange _other) {
            _cachedSize = -1;
            if (_other.hasLowerEndpoint()) {
                setLowerEndpoint().copyFrom(_other.lowerEndpoint);
            }
            if (_other.hasUpperEndpoint()) {
                setUpperEndpoint().copyFrom(_other.upperEndpoint);
            }
            return this;
        }
        public byte[] toByteArray() {
            byte[] a = new byte[getSerializedSize()];
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a).writerIndex(0);
            this.writeTo(b);
            return a;
        }
        public void parseFrom(byte[] a) {
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a);
            this.parseFrom(b, b.readableBytes());
        }
        private int _cachedSize;

        private io.netty.buffer.ByteBuf _parsedBuffer;

    }

    public static final class BatchedEntryDeletionIndexInfo {
        private NestedPositionInfo position;
        private static final int _POSITION_FIELD_NUMBER = 1;
        private static final int _POSITION_TAG = (_POSITION_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _POSITION_TAG_SIZE = LightProtoCodec.computeVarIntSize(_POSITION_TAG);
        private static final int _POSITION_MASK = 1 << (0 % 32);
        public boolean hasPosition() {
            return (_bitField0 & _POSITION_MASK) != 0;
        }
        public NestedPositionInfo getPosition() {
            if (!hasPosition()) {
                throw new IllegalStateException("Field 'position' is not set");
            }
            return position;
        }
        public NestedPositionInfo setPosition() {
            if (position == null) {
                position = new NestedPositionInfo();
            }
            _bitField0 |= _POSITION_MASK;
            _cachedSize = -1;
            return position;
        }
        public BatchedEntryDeletionIndexInfo clearPosition() {
            _bitField0 &= ~_POSITION_MASK;
            if (hasPosition()) {
                position.clear();
            }
            return this;
        }

        private long[] deleteSets = null;
        private int _deleteSetsCount = 0;
        private static final int _DELETE_SET_FIELD_NUMBER = 2;
        private static final int _DELETE_SET_TAG = (_DELETE_SET_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_VARINT;
        private static final int _DELETE_SET_TAG_SIZE = LightProtoCodec.computeVarIntSize(_DELETE_SET_TAG);
        private static final int _DELETE_SET_TAG_PACKED = (_DELETE_SET_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        public int getDeleteSetsCount() {
            return _deleteSetsCount;
        }
        public long getDeleteSetAt(int idx) {
            if (idx < 0 || idx >= _deleteSetsCount) {
                throw new IndexOutOfBoundsException(
                        "Index " + idx + " is out of the list size (" + _deleteSetsCount + ") for field 'deleteSet'");
            }
            return deleteSets[idx];
        }
        public void addDeleteSet(long deleteSet) {
            if (deleteSets == null) {
                deleteSets = new long[4];
            }
            if (deleteSets.length == _deleteSetsCount) {
                deleteSets = java.util.Arrays.copyOf(deleteSets, _deleteSetsCount * 2);
            }
            _cachedSize = -1;
            deleteSets[_deleteSetsCount++] = deleteSet;
        }
        public BatchedEntryDeletionIndexInfo clearDeleteSet() {
            _deleteSetsCount = 0;
            return this;
        }

        private int _bitField0;
        private static final int _REQUIRED_FIELDS_MASK0 = 0 | _POSITION_MASK;
        public int writeTo(io.netty.buffer.ByteBuf _b) {
            checkRequiredFields();
            int _writeIdx = _b.writerIndex();
            LightProtoCodec.writeVarInt(_b, _POSITION_TAG);
            LightProtoCodec.writeVarInt(_b, position.getSerializedSize());
            position.writeTo(_b);
            for (int i = 0; i < _deleteSetsCount; i++) {
                long _item = deleteSets[i];
                LightProtoCodec.writeVarInt(_b, _DELETE_SET_TAG);
                LightProtoCodec.writeVarInt64(_b, _item);
            }
            return (_b.writerIndex() - _writeIdx);
        }
        public int getSerializedSize() {
            if (_cachedSize > -1) {
                return _cachedSize;
            }

            int _size = 0;
            _size += LightProtoCodec.computeVarIntSize(_POSITION_TAG);
            int MsgsizePosition = position.getSerializedSize();
            _size += LightProtoCodec.computeVarIntSize(MsgsizePosition) + MsgsizePosition;
            for (int i = 0; i < _deleteSetsCount; i++) {
                long _item = deleteSets[i];
                _size += _DELETE_SET_TAG_SIZE;
                _size += LightProtoCodec.computeVarInt64Size(_item);
            }
            _cachedSize = _size;
            return _size;
        }
        public void parseFrom(io.netty.buffer.ByteBuf _buffer, int _size) {
            clear();
            int _endIdx = _buffer.readerIndex() + _size;
            while (_buffer.readerIndex() < _endIdx) {
                int _tag = LightProtoCodec.readVarInt(_buffer);
                switch (_tag) {
                    case _POSITION_TAG :
                        _bitField0 |= _POSITION_MASK;
                        int positionSize = LightProtoCodec.readVarInt(_buffer);
                        setPosition().parseFrom(_buffer, positionSize);
                        break;
                    case _DELETE_SET_TAG :
                        addDeleteSet(LightProtoCodec.readVarInt64(_buffer));
                        break;
                    case _DELETE_SET_TAG_PACKED :
                        int _deleteSetSize = LightProtoCodec.readVarInt(_buffer);
                        int _deleteSetEndIdx = _buffer.readerIndex() + _deleteSetSize;
                        while (_buffer.readerIndex() < _deleteSetEndIdx) {
                            addDeleteSet(LightProtoCodec.readVarInt64(_buffer));
                        }
                        break;
                    default :
                        LightProtoCodec.skipUnknownField(_tag, _buffer);
                }
            }
            checkRequiredFields();
            _parsedBuffer = _buffer;
        }
        private void checkRequiredFields() {
            if ((_bitField0 & _REQUIRED_FIELDS_MASK0) != _REQUIRED_FIELDS_MASK0) {
                throw new IllegalStateException("Some required fields are missing");
            }
        }
        public BatchedEntryDeletionIndexInfo clear() {
            if (hasPosition()) {
                position.clear();
            }
            _deleteSetsCount = 0;
            _parsedBuffer = null;
            _cachedSize = -1;
            _bitField0 = 0;
            return this;
        }
        public BatchedEntryDeletionIndexInfo copyFrom(
                BatchedEntryDeletionIndexInfo _other) {
            _cachedSize = -1;
            if (_other.hasPosition()) {
                setPosition().copyFrom(_other.position);
            }
            for (int i = 0; i < _other.getDeleteSetsCount(); i++) {
                addDeleteSet(_other.getDeleteSetAt(i));
            }
            return this;
        }
        public byte[] toByteArray() {
            byte[] a = new byte[getSerializedSize()];
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a).writerIndex(0);
            this.writeTo(b);
            return a;
        }
        public void parseFrom(byte[] a) {
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a);
            this.parseFrom(b, b.readableBytes());
        }
        private int _cachedSize;

        private io.netty.buffer.ByteBuf _parsedBuffer;

    }

    public static final class LongProperty {
        private String name;
        private int _nameBufferIdx = -1;
        private int _nameBufferLen = -1;
        private static final int _NAME_FIELD_NUMBER = 1;
        private static final int _NAME_TAG = (_NAME_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _NAME_TAG_SIZE = LightProtoCodec.computeVarIntSize(_NAME_TAG);
        private static final int _NAME_MASK = 1 << (0 % 32);
        public boolean hasName() {
            return (_bitField0 & _NAME_MASK) != 0;
        }
        public String getName() {
            if (!hasName()) {
                throw new IllegalStateException("Field 'name' is not set");
            }
            if (name == null) {
                name = LightProtoCodec.readString(_parsedBuffer, _nameBufferIdx, _nameBufferLen);
            }
            return name;
        }
        public LongProperty setName(String name) {
            this.name = name;
            _bitField0 |= _NAME_MASK;
            _nameBufferIdx = -1;
            _nameBufferLen = LightProtoCodec.computeStringUTF8Size(name);
            _cachedSize = -1;
            return this;
        }
        public LongProperty clearName() {
            _bitField0 &= ~_NAME_MASK;
            name = null;
            _nameBufferIdx = -1;
            _nameBufferLen = -1;
            return this;
        }

        private long value;
        private static final int _VALUE_FIELD_NUMBER = 2;
        private static final int _VALUE_TAG = (_VALUE_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_VARINT;
        private static final int _VALUE_TAG_SIZE = LightProtoCodec.computeVarIntSize(_VALUE_TAG);
        private static final int _VALUE_MASK = 1 << (1 % 32);
        public boolean hasValue() {
            return (_bitField0 & _VALUE_MASK) != 0;
        }
        public long getValue() {
            if (!hasValue()) {
                throw new IllegalStateException("Field 'value' is not set");
            }
            return value;
        }
        public LongProperty setValue(long value) {
            this.value = value;
            _bitField0 |= _VALUE_MASK;
            _cachedSize = -1;
            return this;
        }
        public LongProperty clearValue() {
            _bitField0 &= ~_VALUE_MASK;
            return this;
        }

        private int _bitField0;
        private static final int _REQUIRED_FIELDS_MASK0 = 0 | _NAME_MASK | _VALUE_MASK;
        public int writeTo(io.netty.buffer.ByteBuf _b) {
            checkRequiredFields();
            int _writeIdx = _b.writerIndex();
            LightProtoCodec.writeVarInt(_b, _NAME_TAG);
            LightProtoCodec.writeVarInt(_b, _nameBufferLen);
            if (_nameBufferIdx == -1) {
                LightProtoCodec.writeString(_b, name, _nameBufferLen);
            } else {
                _parsedBuffer.getBytes(_nameBufferIdx, _b, _nameBufferLen);
            }
            LightProtoCodec.writeVarInt(_b, _VALUE_TAG);
            LightProtoCodec.writeVarInt64(_b, value);
            return (_b.writerIndex() - _writeIdx);
        }
        public int getSerializedSize() {
            if (_cachedSize > -1) {
                return _cachedSize;
            }

            int _size = 0;
            _size += _NAME_TAG_SIZE;
            _size += LightProtoCodec.computeVarIntSize(_nameBufferLen);
            _size += _nameBufferLen;
            _size += _VALUE_TAG_SIZE;
            _size += LightProtoCodec.computeVarInt64Size(value);
            _cachedSize = _size;
            return _size;
        }
        public void parseFrom(io.netty.buffer.ByteBuf _buffer, int _size) {
            clear();
            int _endIdx = _buffer.readerIndex() + _size;
            while (_buffer.readerIndex() < _endIdx) {
                int _tag = LightProtoCodec.readVarInt(_buffer);
                switch (_tag) {
                    case _NAME_TAG :
                        _bitField0 |= _NAME_MASK;
                        _nameBufferLen = LightProtoCodec.readVarInt(_buffer);
                        _nameBufferIdx = _buffer.readerIndex();
                        _buffer.skipBytes(_nameBufferLen);
                        break;
                    case _VALUE_TAG :
                        _bitField0 |= _VALUE_MASK;
                        value = LightProtoCodec.readVarInt64(_buffer);
                        break;
                    default :
                        LightProtoCodec.skipUnknownField(_tag, _buffer);
                }
            }
            checkRequiredFields();
            _parsedBuffer = _buffer;
        }
        private void checkRequiredFields() {
            if ((_bitField0 & _REQUIRED_FIELDS_MASK0) != _REQUIRED_FIELDS_MASK0) {
                throw new IllegalStateException("Some required fields are missing");
            }
        }
        public LongProperty clear() {
            name = null;
            _nameBufferIdx = -1;
            _nameBufferLen = -1;
            _parsedBuffer = null;
            _cachedSize = -1;
            _bitField0 = 0;
            return this;
        }
        public LongProperty copyFrom(LongProperty _other) {
            _cachedSize = -1;
            if (_other.hasName()) {
                setName(_other.getName());
            }
            if (_other.hasValue()) {
                setValue(_other.value);
            }
            return this;
        }
        public byte[] toByteArray() {
            byte[] a = new byte[getSerializedSize()];
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a).writerIndex(0);
            this.writeTo(b);
            return a;
        }
        public void parseFrom(byte[] a) {
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a);
            this.parseFrom(b, b.readableBytes());
        }
        private int _cachedSize;

        private io.netty.buffer.ByteBuf _parsedBuffer;

    }

    public static final class StringProperty {
        private String name;
        private int _nameBufferIdx = -1;
        private int _nameBufferLen = -1;
        private static final int _NAME_FIELD_NUMBER = 1;
        private static final int _NAME_TAG = (_NAME_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _NAME_TAG_SIZE = LightProtoCodec.computeVarIntSize(_NAME_TAG);
        private static final int _NAME_MASK = 1 << (0 % 32);
        public boolean hasName() {
            return (_bitField0 & _NAME_MASK) != 0;
        }
        public String getName() {
            if (!hasName()) {
                throw new IllegalStateException("Field 'name' is not set");
            }
            if (name == null) {
                name = LightProtoCodec.readString(_parsedBuffer, _nameBufferIdx, _nameBufferLen);
            }
            return name;
        }
        public StringProperty setName(String name) {
            this.name = name;
            _bitField0 |= _NAME_MASK;
            _nameBufferIdx = -1;
            _nameBufferLen = LightProtoCodec.computeStringUTF8Size(name);
            _cachedSize = -1;
            return this;
        }
        public StringProperty clearName() {
            _bitField0 &= ~_NAME_MASK;
            name = null;
            _nameBufferIdx = -1;
            _nameBufferLen = -1;
            return this;
        }

        private String value;
        private int _valueBufferIdx = -1;
        private int _valueBufferLen = -1;
        private static final int _VALUE_FIELD_NUMBER = 2;
        private static final int _VALUE_TAG = (_VALUE_FIELD_NUMBER << LightProtoCodec.TAG_TYPE_BITS)
                | LightProtoCodec.WIRETYPE_LENGTH_DELIMITED;
        private static final int _VALUE_TAG_SIZE = LightProtoCodec.computeVarIntSize(_VALUE_TAG);
        private static final int _VALUE_MASK = 1 << (1 % 32);
        public boolean hasValue() {
            return (_bitField0 & _VALUE_MASK) != 0;
        }
        public String getValue() {
            if (!hasValue()) {
                throw new IllegalStateException("Field 'value' is not set");
            }
            if (value == null) {
                value = LightProtoCodec.readString(_parsedBuffer, _valueBufferIdx, _valueBufferLen);
            }
            return value;
        }
        public StringProperty setValue(String value) {
            this.value = value;
            _bitField0 |= _VALUE_MASK;
            _valueBufferIdx = -1;
            _valueBufferLen = LightProtoCodec.computeStringUTF8Size(value);
            _cachedSize = -1;
            return this;
        }
        public StringProperty clearValue() {
            _bitField0 &= ~_VALUE_MASK;
            value = null;
            _valueBufferIdx = -1;
            _valueBufferLen = -1;
            return this;
        }

        private int _bitField0;
        private static final int _REQUIRED_FIELDS_MASK0 = 0 | _NAME_MASK | _VALUE_MASK;
        public int writeTo(io.netty.buffer.ByteBuf _b) {
            checkRequiredFields();
            int _writeIdx = _b.writerIndex();
            LightProtoCodec.writeVarInt(_b, _NAME_TAG);
            LightProtoCodec.writeVarInt(_b, _nameBufferLen);
            if (_nameBufferIdx == -1) {
                LightProtoCodec.writeString(_b, name, _nameBufferLen);
            } else {
                _parsedBuffer.getBytes(_nameBufferIdx, _b, _nameBufferLen);
            }
            LightProtoCodec.writeVarInt(_b, _VALUE_TAG);
            LightProtoCodec.writeVarInt(_b, _valueBufferLen);
            if (_valueBufferIdx == -1) {
                LightProtoCodec.writeString(_b, value, _valueBufferLen);
            } else {
                _parsedBuffer.getBytes(_valueBufferIdx, _b, _valueBufferLen);
            }
            return (_b.writerIndex() - _writeIdx);
        }
        public int getSerializedSize() {
            if (_cachedSize > -1) {
                return _cachedSize;
            }

            int _size = 0;
            _size += _NAME_TAG_SIZE;
            _size += LightProtoCodec.computeVarIntSize(_nameBufferLen);
            _size += _nameBufferLen;
            _size += _VALUE_TAG_SIZE;
            _size += LightProtoCodec.computeVarIntSize(_valueBufferLen);
            _size += _valueBufferLen;
            _cachedSize = _size;
            return _size;
        }
        public void parseFrom(io.netty.buffer.ByteBuf _buffer, int _size) {
            clear();
            int _endIdx = _buffer.readerIndex() + _size;
            while (_buffer.readerIndex() < _endIdx) {
                int _tag = LightProtoCodec.readVarInt(_buffer);
                switch (_tag) {
                    case _NAME_TAG :
                        _bitField0 |= _NAME_MASK;
                        _nameBufferLen = LightProtoCodec.readVarInt(_buffer);
                        _nameBufferIdx = _buffer.readerIndex();
                        _buffer.skipBytes(_nameBufferLen);
                        break;
                    case _VALUE_TAG :
                        _bitField0 |= _VALUE_MASK;
                        _valueBufferLen = LightProtoCodec.readVarInt(_buffer);
                        _valueBufferIdx = _buffer.readerIndex();
                        _buffer.skipBytes(_valueBufferLen);
                        break;
                    default :
                        LightProtoCodec.skipUnknownField(_tag, _buffer);
                }
            }
            checkRequiredFields();
            _parsedBuffer = _buffer;
        }
        private void checkRequiredFields() {
            if ((_bitField0 & _REQUIRED_FIELDS_MASK0) != _REQUIRED_FIELDS_MASK0) {
                throw new IllegalStateException("Some required fields are missing");
            }
        }
        public StringProperty clear() {
            name = null;
            _nameBufferIdx = -1;
            _nameBufferLen = -1;
            value = null;
            _valueBufferIdx = -1;
            _valueBufferLen = -1;
            _parsedBuffer = null;
            _cachedSize = -1;
            _bitField0 = 0;
            return this;
        }
        public StringProperty copyFrom(StringProperty _other) {
            _cachedSize = -1;
            if (_other.hasName()) {
                setName(_other.getName());
            }
            if (_other.hasValue()) {
                setValue(_other.getValue());
            }
            return this;
        }
        public byte[] toByteArray() {
            byte[] a = new byte[getSerializedSize()];
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a).writerIndex(0);
            this.writeTo(b);
            return a;
        }
        public void parseFrom(byte[] a) {
            io.netty.buffer.ByteBuf b = io.netty.buffer.Unpooled.wrappedBuffer(a);
            this.parseFrom(b, b.readableBytes());
        }
        private int _cachedSize;

        private io.netty.buffer.ByteBuf _parsedBuffer;

    }



    static final class LightProtoCodec {
        static final int TAG_TYPE_MASK = 7;
        static final int TAG_TYPE_BITS = 3;
        static final int WIRETYPE_VARINT = 0;
        static final int WIRETYPE_FIXED64 = 1;
        static final int WIRETYPE_LENGTH_DELIMITED = 2;
        static final int WIRETYPE_START_GROUP = 3;
        static final int WIRETYPE_END_GROUP = 4;
        static final int WIRETYPE_FIXED32 = 5;
        private LightProtoCodec() {
        }

        private static int getTagType(int tag) {
            return tag & TAG_TYPE_MASK;
        }

        static int getFieldId(int tag) {
            return tag >>> TAG_TYPE_BITS;
        }

        static void writeVarInt(ByteBuf b, int n) {
            if (n >= 0) {
                _writeVarInt(b, n);
            } else {
                writeVarInt64(b, n);
            }
        }

        static void writeSignedVarInt(ByteBuf b, int n) {
            writeVarInt(b, encodeZigZag32(n));
        }

        static int readSignedVarInt(ByteBuf b) {
            return decodeZigZag32(readVarInt(b));
        }

        static long readSignedVarInt64(ByteBuf b) {
            return decodeZigZag64(readVarInt64(b));
        }

        static void writeFloat(ByteBuf b, float n) {
            writeFixedInt32(b, Float.floatToRawIntBits(n));
        }

        static void writeDouble(ByteBuf b, double n) {
            writeFixedInt64(b, Double.doubleToRawLongBits(n));
        }

        static float readFloat(ByteBuf b) {
            return Float.intBitsToFloat(readFixedInt32(b));
        }

        static double readDouble(ByteBuf b) {
            return Double.longBitsToDouble(readFixedInt64(b));
        }

        private static void _writeVarInt(ByteBuf b, int n) {
            while (true) {
                if ((n & ~0x7F) == 0) {
                    b.writeByte(n);
                    return;
                } else {
                    b.writeByte((n & 0x7F) | 0x80);
                    n >>>= 7;
                }
            }
        }

        static void writeVarInt64(ByteBuf b, long value) {
            while (true) {
                if ((value & ~0x7FL) == 0) {
                    b.writeByte((int) value);
                    return;
                } else {
                    b.writeByte(((int) value & 0x7F) | 0x80);
                    value >>>= 7;
                }
            }
        }

        static void writeFixedInt32(ByteBuf b, int n) {
            b.writeIntLE(n);
        }

        static void writeFixedInt64(ByteBuf b, long n) {
            b.writeLongLE(n);
        }

        static int readFixedInt32(ByteBuf b) {
            return b.readIntLE();
        }

        static long readFixedInt64(ByteBuf b) {
            return b.readLongLE();
        }

        static void writeSignedVarInt64(ByteBuf b, long n) {
            writeVarInt64(b, encodeZigZag64(n));
        }

        private static int encodeZigZag32(final int n) {
            return (n << 1) ^ (n >> 31);
        }

        private static long encodeZigZag64(final long n) {
            return (n << 1) ^ (n >> 63);
        }

        private static int decodeZigZag32(int n) {
            return n >>> 1 ^ -(n & 1);
        }

        private static long decodeZigZag64(long n) {
            return n >>> 1 ^ -(n & 1L);
        }

        static int readVarInt(ByteBuf buf) {
            byte tmp = buf.readByte();
            if (tmp >= 0) {
                return tmp;
            }
            int result = tmp & 0x7f;
            if ((tmp = buf.readByte()) >= 0) {
                result |= tmp << 7;
            } else {
                result |= (tmp & 0x7f) << 7;
                if ((tmp = buf.readByte()) >= 0) {
                    result |= tmp << 14;
                } else {
                    result |= (tmp & 0x7f) << 14;
                    if ((tmp = buf.readByte()) >= 0) {
                        result |= tmp << 21;
                    } else {
                        result |= (tmp & 0x7f) << 21;
                        result |= (tmp = buf.readByte()) << 28;
                        if (tmp < 0) {
                            // Discard upper 32 bits.
                            for (int i = 0; i < 5; i++) {
                                if (buf.readByte() >= 0) {
                                    return result;
                                }
                            }
                            throw new IllegalArgumentException("Encountered a malformed varint.");
                        }
                    }
                }
            }
            return result;
        }

        static long readVarInt64(ByteBuf buf) {
            int shift = 0;
            long result = 0;
            while (shift < 64) {
                final byte b = buf.readByte();
                result |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
                shift += 7;
            }
            throw new IllegalArgumentException("Encountered a malformed varint.");
        }

        static int computeSignedVarIntSize(final int value) {
            return computeVarUIntSize(encodeZigZag32(value));
        }

        static int computeSignedVarInt64Size(final long value) {
            return computeVarInt64Size(encodeZigZag64(value));
        }

        static int computeVarIntSize(final int value) {
            if (value < 0) {
                return 10;
            } else {
                return computeVarUIntSize(value);
            }
        }

        static int computeVarUIntSize(final int value) {
            if ((value & (0xffffffff << 7)) == 0) {
                return 1;
            } else if ((value & (0xffffffff << 14)) == 0) {
                return 2;
            } else if ((value & (0xffffffff << 21)) == 0) {
                return 3;
            } else if ((value & (0xffffffff << 28)) == 0) {
                return 4;
            } else {
                return 5;
            }
        }

        static int computeVarInt64Size(final long value) {
            if ((value & (0xffffffffffffffffL << 7)) == 0) {
                return 1;
            } else if ((value & (0xffffffffffffffffL << 14)) == 0) {
                return 2;
            } else if ((value & (0xffffffffffffffffL << 21)) == 0) {
                return 3;
            } else if ((value & (0xffffffffffffffffL << 28)) == 0) {
                return 4;
            } else if ((value & (0xffffffffffffffffL << 35)) == 0) {
                return 5;
            } else if ((value & (0xffffffffffffffffL << 42)) == 0) {
                return 6;
            } else if ((value & (0xffffffffffffffffL << 49)) == 0) {
                return 7;
            } else if ((value & (0xffffffffffffffffL << 56)) == 0) {
                return 8;
            } else if ((value & (0xffffffffffffffffL << 63)) == 0) {
                return 9;
            } else {
                return 10;
            }
        }

        static int computeStringUTF8Size(String s) {
            return ByteBufUtil.utf8Bytes(s);
        }

        static void writeString(ByteBuf b, String s, int bytesCount) {
            ByteBufUtil.reserveAndWriteUtf8(b, s, bytesCount);
        }

        static String readString(ByteBuf b, int index, int len) {
            return b.toString(index, len, StandardCharsets.UTF_8);
        }

        static void skipUnknownField(int tag, ByteBuf buffer) {
            int tagType = getTagType(tag);
            switch (tagType) {
                case WIRETYPE_VARINT :
                    readVarInt(buffer);
                    break;
                case WIRETYPE_FIXED64 :
                    buffer.skipBytes(8);
                    break;
                case WIRETYPE_LENGTH_DELIMITED :
                    int len = readVarInt(buffer);
                    buffer.skipBytes(len);
                    break;
                case WIRETYPE_FIXED32 :
                    buffer.skipBytes(4);
                    break;
                default :
                    throw new IllegalArgumentException("Invalid unknonwn tag type: " + tagType);
            }
        }

        static final class StringHolder {
            String s;
            int idx;
            int len;
        }

        static final class BytesHolder {
            ByteBuf b;
            int idx;
            int len;
        }
    }

}
