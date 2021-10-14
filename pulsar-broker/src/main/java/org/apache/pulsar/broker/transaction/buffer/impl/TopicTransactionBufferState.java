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
package org.apache.pulsar.broker.transaction.buffer.impl;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * The implement of topic transaction buffer state.
 */
public abstract class TopicTransactionBufferState {

    /**
     * The state of the topicTransactionBuffer {@link TopicTransactionBuffer}.
     */
    public enum State {
        None,
        Initializing,
        Ready,
        Close,
        NoSnapshot
    }

    private static final AtomicReferenceFieldUpdater<TopicTransactionBufferState, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(TopicTransactionBufferState.class, State.class, "state");

    @SuppressWarnings("unused")
    private volatile State state = null;

    public TopicTransactionBufferState(State state) {
        STATE_UPDATER.set(this, state);
    }

    protected boolean changeToReadyState() {
        return (STATE_UPDATER.compareAndSet(this, State.Initializing, State.Ready));
    }

    protected boolean changeToNoSnapshotState() {
        return (STATE_UPDATER.compareAndSet(this, State.Initializing, State.NoSnapshot));
    }

    protected boolean changeToInitializingState() {
        return STATE_UPDATER.compareAndSet(this, State.None, State.Initializing);
    }

    protected boolean changeToReadyStateFromNoSnapshot() {
        return STATE_UPDATER.compareAndSet(this, State.NoSnapshot, State.Ready);
    }

    protected boolean changeToCloseState() {
        return (STATE_UPDATER.compareAndSet(this, State.Ready, State.Close)
                || STATE_UPDATER.compareAndSet(this, State.None, State.Close)
                || STATE_UPDATER.compareAndSet(this, State.Initializing, State.Close)
                || STATE_UPDATER.compareAndSet(this, State.NoSnapshot, State.Close));
    }

    public boolean checkIfReady() {
        return STATE_UPDATER.get(this) == State.Ready;
    }

    public boolean checkIfNoSnapshot() {
        return STATE_UPDATER.get(this) == State.NoSnapshot;
    }

    public State getState() {
        return STATE_UPDATER.get(this);
    }
}