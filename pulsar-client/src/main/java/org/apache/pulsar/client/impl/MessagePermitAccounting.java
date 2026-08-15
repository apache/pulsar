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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.common.api.proto.CommandMessage;

/**
 * Resolves the permit count represented by a message command and tracks that permit budget while a batch is decoded.
 */
final class MessagePermitAccounting {

    private MessagePermitAccounting() {
    }

    static int resolveForEarlyFailure(CommandMessage command) {
        if (command.hasMessagePermits()) {
            return getExplicitMessagePermits(command);
        }
        return command.getAckSetsCount() > 0 ? getAckSetCardinality(command, -1) : 1;
    }

    static int resolve(CommandMessage command, int batchSize) {
        int expectedPermits = command.getAckSetsCount() > 0
                ? getAckSetCardinality(command, batchSize) : batchSize;
        if (!command.hasMessagePermits()) {
            return expectedPermits;
        }

        int explicitPermits = getExplicitMessagePermits(command);
        if (explicitPermits != expectedPermits) {
            throw new InvalidMessagePermitsException("Explicit message permits " + explicitPermits
                    + " do not match the payload and ack set value " + expectedPermits);
        }
        return explicitPermits;
    }

    private static int getExplicitMessagePermits(CommandMessage command) {
        long messagePermits = Integer.toUnsignedLong(command.getMessagePermits());
        if (messagePermits == 0 || messagePermits > Integer.MAX_VALUE) {
            throw new InvalidMessagePermitsException("Invalid explicit message permits " + messagePermits);
        }
        return (int) messagePermits;
    }

    private static int getAckSetCardinality(CommandMessage command, int batchSize) {
        int words = command.getAckSetsCount();
        int completeWords = batchSize < 0 ? words : Math.min(batchSize >>> 6, words);
        long cardinality = 0;
        for (int i = 0; i < completeWords; i++) {
            cardinality += Long.bitCount(command.getAckSetAt(i));
        }
        if (batchSize >= 0) {
            int remainingBits = batchSize & 63;
            if (remainingBits > 0 && completeWords < words) {
                long mask = -1L >>> (Long.SIZE - remainingBits);
                cardinality += Long.bitCount(command.getAckSetAt(completeWords) & mask);
            }
        }
        if (cardinality > Integer.MAX_VALUE) {
            throw new InvalidMessagePermitsException("Ack set message permits exceed the supported range");
        }
        return (int) cardinality;
    }

    static final class Budget {
        private int remainingPermits;

        Budget(int messagePermits) {
            remainingPermits = messagePermits;
        }

        void claim() {
            if (remainingPermits == 0) {
                throw new InvalidMessagePermitsException("Batch contains more deliverable messages than permits");
            }
            remainingPermits--;
        }

        void restore() {
            remainingPermits++;
        }

        int drain() {
            int permits = remainingPermits;
            remainingPermits = 0;
            return permits;
        }
    }

    static final class InvalidMessagePermitsException extends IllegalStateException {
        InvalidMessagePermitsException(String message) {
            super(message);
        }
    }
}
