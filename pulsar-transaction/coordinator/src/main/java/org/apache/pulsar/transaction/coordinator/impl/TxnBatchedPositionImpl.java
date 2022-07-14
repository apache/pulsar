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
package org.apache.pulsar.transaction.coordinator.impl;

import java.util.Objects;
import lombok.Getter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

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

    public TxnBatchedPositionImpl(Position position, int batchSize, int batchIndex, long[] ackSet){
        super(position.getLedgerId(), position.getEntryId(), ackSet);
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TxnBatchedPositionImpl other) {
            return super.equals(o) && batchSize == other.batchSize && batchIndex == other.batchIndex;
        }
        return false;

    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), batchSize, batchIndex);
    }
}
