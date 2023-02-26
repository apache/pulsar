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
package org.apache.bookkeeper.mledger.util;

import com.google.common.collect.ComparisonChain;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.util.collections.BitSetRecyclable;

public class PositionAckSetUtil {

    //This method is to compare two ack set whether overlap or not
    public static boolean isAckSetOverlap(long[] currentAckSet, long[] otherAckSet) {
        if (currentAckSet == null || otherAckSet == null) {
            return false;
        }

        BitSetRecyclable currentBitSet = BitSetRecyclable.valueOf(currentAckSet);
        BitSetRecyclable otherBitSet = BitSetRecyclable.valueOf(otherAckSet);
        currentBitSet.flip(0, currentBitSet.size());
        otherBitSet.flip(0, otherBitSet.size());
        currentBitSet.and(otherBitSet);
        boolean isAckSetRepeated = !currentBitSet.isEmpty();
        currentBitSet.recycle();
        otherBitSet.recycle();
        return isAckSetRepeated;
    }

    //This method is do `and` operation for position's ack set
    public static void andAckSet(PositionImpl currentPosition, PositionImpl otherPosition) {
        if (currentPosition == null || otherPosition == null) {
            return;
        }
        currentPosition.setAckSet(andAckSet(currentPosition.getAckSet(), otherPosition.getAckSet()));
    }

    //This method is do `and` operation for ack set
    public static long[] andAckSet(long[] firstAckSet, long[] secondAckSet) {
        BitSetRecyclable thisAckSet = BitSetRecyclable.valueOf(firstAckSet);
        BitSetRecyclable otherAckSet = BitSetRecyclable.valueOf(secondAckSet);
        thisAckSet.and(otherAckSet);
        long[] ackSet = thisAckSet.toLongArray();
        thisAckSet.recycle();
        otherAckSet.recycle();
        return ackSet;
    }

    public static boolean isAckSetEmpty(long[] ackSet) {
        BitSetRecyclable bitSet =  BitSetRecyclable.create().resetWords(ackSet);
        boolean isEmpty = bitSet.isEmpty();
        bitSet.recycle();
        return isEmpty;
    }

    //This method is compare two position which position is bigger than another one.
    //When the ledgerId and entryId in this position is same to another one and two position all have ack set, it will
    //compare the ack set next bit index is bigger than another one.
    public static int compareToWithAckSet(PositionImpl currentPosition, PositionImpl otherPosition) {
        if (currentPosition == null || otherPosition == null) {
            throw new IllegalArgumentException("Two positions can't be null! "
                    + "current position : [" + currentPosition + "] other position : [" + otherPosition + "]");
        }
        int result = ComparisonChain.start().compare(currentPosition.getLedgerId(),
                otherPosition.getLedgerId()).compare(currentPosition.getEntryId(), otherPosition.getEntryId())
                .result();
        if (result == 0) {
            BitSetRecyclable otherAckSet;
            BitSetRecyclable currentAckSet;

            if (otherPosition.getAckSet() == null) {
                otherAckSet = BitSetRecyclable.create();
            } else {
                otherAckSet = BitSetRecyclable.valueOf(otherPosition.getAckSet());
            }

            if (currentPosition.getAckSet() == null) {
                currentAckSet = BitSetRecyclable.create();
            } else {
                currentAckSet = BitSetRecyclable.valueOf(currentPosition.getAckSet());
            }

            if (currentAckSet.isEmpty() || otherAckSet.isEmpty()) {
                //when ack set is empty, the nextSetBit will return -1, so we should return the inverse value.
                result = -(currentAckSet.nextSetBit(0) - otherAckSet.nextSetBit(0));
            } else {
                result = currentAckSet.nextSetBit(0) - otherAckSet.nextSetBit(0);
            }
            currentAckSet.recycle();
            otherAckSet.recycle();
        }
        return result;
    }

}
