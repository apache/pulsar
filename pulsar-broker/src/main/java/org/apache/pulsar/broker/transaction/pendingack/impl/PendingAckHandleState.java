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
package org.apache.pulsar.broker.transaction.pendingack.impl;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * The implement of transaction pending ack store.
 */
public abstract class PendingAckHandleState {

    /**
     * The state of the pending ack handle {@link PendingAckHandleState}.
     */
    public enum State {
        None,
        Initializing,
        Ready,
        Error,
        Close
    }

    private static final AtomicReferenceFieldUpdater<PendingAckHandleState, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(PendingAckHandleState.class, State.class, "state");

    @SuppressWarnings("unused")
    protected volatile State state = null;

    public PendingAckHandleState(State state) {
        STATE_UPDATER.set(this, state);

    }

    protected boolean changeToReadyState() {
        return (STATE_UPDATER.compareAndSet(this, State.Initializing, State.Ready));
    }

    protected boolean changeToInitializingState() {
        return STATE_UPDATER.compareAndSet(this, State.None, State.Initializing);
    }

    protected void changeToCloseState() {
        STATE_UPDATER.set(this, State.Close);
    }

    protected void changeToErrorState() {
        STATE_UPDATER.set(this, State.Error);
    }

    public boolean checkIfReady() {
        return STATE_UPDATER.get(this) == State.Ready;
    }

    public boolean checkIfClose() {
        return STATE_UPDATER.get(this) == State.Close;
    }

    public State getState() {
        return STATE_UPDATER.get(this);
    }
}