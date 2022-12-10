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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class ClientCnxIdleState {

    private final ClientCnx clientCnx;

    /** Stat. **/
    private volatile State state;

    /** Create time. This field is only used to troubleshoot (analyze by dump file). **/
    private final long createTime;

    /** The time when marks the connection is idle. **/
    private long idleMarkTime;

    public ClientCnxIdleState(ClientCnx clientCnx){
        this.clientCnx = clientCnx;
        this.createTime = System.currentTimeMillis();
        this.state = State.USING;
    }

    private static final AtomicReferenceFieldUpdater<ClientCnxIdleState, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ClientCnxIdleState.class, State.class, "state");

    /**
     * Indicates the usage status of the connection and whether it has been released.
     */
    public enum State {
        /** It was using at the time of last check. At current time may be idle. **/
        USING,
        /** The connection is in idle. **/
        IDLE,
        /** The connection is in idle and will be released soon. In this state, the connection can still be used. **/
        RELEASING,
        /** The connection has already been released. **/
        RELEASED;
    }

    /**
     * Get idle-stat.
     * @return connection idle-stat
     */
    public State getIdleStat() {
        return STATE_UPDATER.get(this);
    }
    /**
     * Compare and switch idle-stat.
     * @return Whether the update is successful.Because there may be other threads competing, possible return false.
     */
    boolean compareAndSetIdleStat(State originalStat, State newStat) {
        return STATE_UPDATER.compareAndSet(this, originalStat, newStat);
    }

    /**
     * @return Whether this connection is in use.
     */
    public boolean isUsing() {
        return getIdleStat() == State.USING;
    }

    /**
     * @return Whether this connection is in idle.
     */
    public boolean isIdle() {
        return getIdleStat() == State.IDLE;
    }

    /**
     * @return Whether this connection is in idle and will be released soon.
     */
    public boolean isReleasing() {
        return getIdleStat() == State.RELEASING;
    }

    /**
     * @return Whether this connection has already been released.
     */
    public boolean isReleased() {
        return getIdleStat() == State.RELEASED;
    }

    /**
     * Try to transform the state of the connection to #{@link State#IDLE}, state should only be
     * transformed to #{@link State#IDLE} from state  #{@link State#USING}. if the state
     * is successfully transformed, "idleMarkTime" will be  assigned to current time.
     */
    public void tryMarkIdleAndInitIdleTime() {
        if (compareAndSetIdleStat(State.USING, State.IDLE)) {
            idleMarkTime = System.currentTimeMillis();
        }
    }

    /**
     * Changes the idle-state of the connection to #{@link State#USING} as much as possible, This method
     * is used when connection borrow, and reset {@link #idleMarkTime} if change state to
     * #{@link State#USING} success.
     * @return Whether change idle-stat to #{@link State#USING} success. False is returned only if the
     * connection has already been released.
     */
    public boolean tryMarkUsingAndClearIdleTime() {
        while (true) {
            // Ensure not released
            if (isReleased()) {
                return false;
            }
            // Try mark release
            if (compareAndSetIdleStat(State.IDLE, State.USING)) {
                idleMarkTime = 0;
                return true;
            }
            if (compareAndSetIdleStat(State.RELEASING, State.USING)) {
                idleMarkTime = 0;
                return true;
            }
            if (isUsing()){
                return true;
            }
        }
    }

    /**
     * Changes the idle-state of the connection to #{@link State#RELEASING}, This method only changes this
     * connection from the #{@link State#IDLE} state to the #{@link State#RELEASING} state.
     * @return Whether change idle-stat to #{@link State#RELEASING} success.
     */
    public boolean tryMarkReleasing() {
        return compareAndSetIdleStat(State.IDLE, State.RELEASING);
    }

    /**
     * Changes the idle-state of the connection to #{@link State#RELEASED}, This method only changes this
     * connection from the #{@link State#RELEASING} state to the #{@link State#RELEASED}
     * state, and close {@param clientCnx} if change state to #{@link State#RELEASED} success.
     * @return Whether change idle-stat to #{@link State#RELEASED} and close connection success.
     */
    public boolean tryMarkReleasedAndCloseConnection() {
        if (!compareAndSetIdleStat(State.RELEASING, State.RELEASED)) {
            return false;
        }
        clientCnx.close();
        return true;
    }

    /**
     * Check whether the connection is idle, and if so, set the idle-state to #{@link State#IDLE}.
     * If the state is already idle and the {@param maxIdleSeconds} is reached, set the state to
     * #{@link State#RELEASING}.
     */
    public void doIdleDetect(long maxIdleSeconds) {
        if (isReleasing()) {
            return;
        }
        if (isIdle()) {
            if (maxIdleSeconds * 1000 + idleMarkTime < System.currentTimeMillis()) {
                tryMarkReleasing();
            }
            return;
        }
        if (clientCnx.idleCheck()) {
            tryMarkIdleAndInitIdleTime();
        }
    }
}
