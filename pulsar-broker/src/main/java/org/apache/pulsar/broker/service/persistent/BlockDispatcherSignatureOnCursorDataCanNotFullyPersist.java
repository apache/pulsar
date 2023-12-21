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
package org.apache.pulsar.broker.service.persistent;

/***
 * A signature that relate to the check of "Dispatching has paused on cursor data can fully persist".
 * Note: do not use this field to confirm whether the delivery should be paused,
 *      please call {@link PersistentDispatcherMultipleConsumers#shouldPauseOnAckStatePersist}.
 */
public class BlockDispatcherSignatureOnCursorDataCanNotFullyPersist {

    /**
     * Used to mark that dispatching was paused at least once in the earlier time, due to the cursor data can not be
     * fully persistent.
     * Why need this filed? It just prevents that
     * {@link PersistentDispatcherMultipleConsumers#afterAckMessages(Throwable, Object)} calls
     * {@link PersistentDispatcherMultipleConsumers#readMoreEntries()} every time, it can avoid too many CPU circles.
     * We just call {@link PersistentDispatcherMultipleConsumers#readMoreEntries()} after the dispatching has been
     * paused at least once earlier.
     */
    private volatile boolean markerAtLeastPausedOnce;

    /**
     * Used to mark some acknowledgements were executed.
     * Because there is a race condition might cause dispatching stuck, the steps to reproduce the issue is like below:
     * - {@link #markerAtLeastPausedOnce} is "false" now.
     * - Thread-reading-entries: there are too many ack holes, so start to pause dispatching
     * - Thread-ack: acked all messages.
     * -             Since {@link #markerAtLeastPausedOnce} is "false", skip to trigger a new reading.
     * - Thread-reading-entries: Set {@link #markerAtLeastPausedOnce} to "true" and discard the reading task.
     * - No longer to trigger a new reading.
     * So we add this field to solve the issue:
     * - Check ack holes in {@link org.apache.bookkeeper.mledger.impl.ManagedCursorImpl#individualDeletedMessages}.
     * - If there is any new acknowledgements when doing the check, redo the check.
     */
    private volatile boolean markerNewAcknowledged;

    public boolean hasNewAcknowledged() {
        return markerNewAcknowledged;
    }

    public boolean hasPausedAtLeastOnce() {
        return markerAtLeastPausedOnce;
    }

    /** Calling when any messages have been acked. **/
    public void markNewAcknowledged() {
        /**
         * Why not use `compare and swap` here?
         * If "markNewAcknowledged" has been override by a "clearMakerNewAcknowledged", it represents that the check
         * {@link org.apache.bookkeeper.mledger.ManagedCursor#isCursorDataFullyPersistable()} runs after
         * {@link PersistentDispatcherMultipleConsumers#afterAckMessages(Throwable, Object)}, so the thread of read more
         * entries would get the newest value of
         * {@link org.apache.bookkeeper.mledger.impl.ManagedCursorImpl#individualDeletedMessages}, so the result of
         * {@link PersistentDispatcherMultipleConsumers#shouldPauseOnAckStatePersist} would be correct.
         */
        markerNewAcknowledged = true;
    }

    public void clearMakerNewAcknowledged() {
        /**
         * Why not use `compare and swap` here?
         * If "clearMakerNewAcknowledged" has been override by a "markNewAcknowledged", do not worry, it just might
         * trigger a new loop for of the method
         * {@link PersistentDispatcherMultipleConsumers#shouldPauseOnAckStatePersist}. Everything would be right.
         */
        markerNewAcknowledged = false;
    }

    /** Calling when a dispatching discarded due to cursor data can not be fully persistent. **/
    public void markPaused() {
        /**
         * Why not use `compare and swap` here?
         * If "markPaused" has been override by a "clearMarkerAtLeastPausedOnce", it means the method
         * {@link PersistentDispatcherMultipleConsumers#afterAckMessages(Throwable, Object)} will trigger a new reading,
         * and the state "markerAtLeastPausedOnce" will be reset by the new reading.
         */
        markerAtLeastPausedOnce = true;
    }

    public void clearMarkerPaused() {
        /**
         * Why not use `compare and swap` here?
         * If "clearMarkerAtLeastPausedOnce" has been override by a "markPaused", it just effects that the next
         * {@link PersistentDispatcherMultipleConsumers#afterAckMessages(Throwable, Object)} will trigger a new reading
         * caused by the wrong state "markerAtLeastPausedOnce". It is not important.
         */
        markerAtLeastPausedOnce = false;
    }
}
