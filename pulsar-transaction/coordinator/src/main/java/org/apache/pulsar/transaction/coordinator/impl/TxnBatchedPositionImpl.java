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
package org.apache.pulsar.transaction.coordinator.impl;

import lombok.Getter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;

/***
 * The difference with {@link PositionImpl} is that there are two more parameters:
 * {@link #batchSize}, {@link #batchIndex}.
 */
public class TxnBatchedPositionImpl extends PositionImpl {

    /** The data length of current batch. **/
    @Getter
    private final int batchSize;

    /** The position of current batch. **/
    @Getter
    private final int batchIndex;

    public TxnBatchedPositionImpl(long ledgerId, long entryId, int batchSize, int batchIndex){
        super(ledgerId, entryId);
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
    }

    public TxnBatchedPositionImpl(Position position, int batchSize, int batchIndex){
        this(position.getLedgerId(), position.getEntryId(), batchSize, batchIndex);
    }

    /**
     * It's exactly the same as {@link PositionImpl}，make sure that when {@link TxnBatchedPositionImpl} used as the key
     * of map same as {@link PositionImpl}. {@link #batchSize} and {@link #batchIndex} should not be involved in
     * calculate, just like {@link PositionImpl#ackSet} is not involved in calculate.
     * Note: In {@link java.util.concurrent.ConcurrentSkipListMap}, it use the {@link Comparable#compareTo(Object)} to
     *   determine whether the keys are the same. In {@link java.util.HashMap}, it use the
     *   {@link Object#hashCode()} & {@link  Object#equals(Object)} to determine whether the keys are the same.
     */
    @Override
    public boolean equals(Object o) {
        return super.equals(o);

    }

    /**
     * It's exactly the same as {@link PositionImpl}，make sure that when {@link TxnBatchedPositionImpl} used as the key
     * of map same as {@link PositionImpl}. {@link #batchSize} and {@link #batchIndex} should not be involved in
     * calculate, just like {@link PositionImpl#ackSet} is not involved in calculate.
     * Note: In {@link java.util.concurrent.ConcurrentSkipListMap}, it use the {@link Comparable#compareTo(Object)} to
     *   determine whether the keys are the same. In {@link java.util.HashMap}, it use the
     *   {@link Object#hashCode()} & {@link  Object#equals(Object)} to determine whether the keys are the same.
     */
    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * It's exactly the same as {@link PositionImpl}，to make sure that when compare to the "markDeletePosition", it
     * looks like {@link PositionImpl}. {@link #batchSize} and {@link #batchIndex} should not be involved in calculate,
     * just like {@link PositionImpl#ackSet} is not involved in calculate.
     * Note: In {@link java.util.concurrent.ConcurrentSkipListMap}, it use the {@link Comparable#compareTo(Object)} to
     *    determine whether the keys are the same. In {@link java.util.HashMap}, it use the
     *    {@link Object#hashCode()} & {@link  Object#equals(Object)} to determine whether the keys are the same.
     */
    public int compareTo(PositionImpl that) {
        return super.compareTo(that);
    }

    /**
     * Build the attribute ackSet to that {@link #batchIndex} is false and others is true.
     */
    public void setAckSetByIndex(){
        if (batchSize == 1){
            return;
        }
        BitSetRecyclable bitSetRecyclable = BitSetRecyclable.create();
        bitSetRecyclable.set(0, batchSize, true);
        bitSetRecyclable.clear(batchIndex);
        long[] ackSet = bitSetRecyclable.toLongArray();
        bitSetRecyclable.recycle();
        setAckSet(ackSet);
    }
}
