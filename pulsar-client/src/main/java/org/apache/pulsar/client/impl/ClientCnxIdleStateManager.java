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
package org.apache.pulsar.client.impl;

public class ClientCnxIdleStateManager {

    /**
     * @return Whether this connection is in use.
     */
    public static boolean isUsing(ClientCnx clientCnx) {
        return clientCnx.getIdleStat() == ClientCnx.IdleState.USING;
    }

    /**
     * @return Whether this connection is in idle.
     */
    public static boolean isIdle(ClientCnx clientCnx) {
        return clientCnx.getIdleStat() == ClientCnx.IdleState.IDLE;
    }

    /**
     * @return Whether this connection is in idle and will be released soon.
     */
    public static boolean isReleasing(ClientCnx clientCnx) {
        return clientCnx.getIdleStat() == ClientCnx.IdleState.RELEASING;
    }

    /**
     * @return Whether this connection has already been released.
     */
    public static boolean isReleased(ClientCnx clientCnx) {
        return clientCnx.getIdleStat() == ClientCnx.IdleState.RELEASED;
    }

    /**
     * Try to transform the state of the connection to #{@link ClientCnx.IdleState#IDLE}, state should only be
     * transformed to #{@link ClientCnx.IdleState#IDLE} from state  #{@link ClientCnx.IdleState#USING}. if the state
     * is successfully transformed, "idleMarkTime" will be  assigned to current time.
     */
    public static void tryMarkIdleAndInitIdleTime(ClientCnx clientCnx) {
        if (clientCnx.compareAndSetIdleStat(ClientCnx.IdleState.USING, ClientCnx.IdleState.IDLE)) {
            clientCnx.idleMarkTime = System.currentTimeMillis();
        }
    }

    /**
     * Changes the idle-state of the connection to #{@link ClientCnx.IdleState#USING} as much as possible, This method
     * is used when connection borrow, and reset {@link ClientCnx#idleMarkTime} if change state to
     * #{@link ClientCnx.IdleState#USING} success.
     * @return Whether change idle-stat to #{@link ClientCnx.IdleState#USING} success. False is returned only if the
     * connection has already been released.
     */
    public static boolean tryMarkUsingAndClearIdleTime(ClientCnx clientCnx) {
        while (true) {
            // Ensure not released
            if (isReleased(clientCnx)) {
                return false;
            }
            // Try mark release
            if (clientCnx.compareAndSetIdleStat(ClientCnx.IdleState.IDLE, ClientCnx.IdleState.USING)) {
                clientCnx.idleMarkTime = 0;
                return true;
            }
            if (clientCnx.compareAndSetIdleStat(ClientCnx.IdleState.RELEASING, ClientCnx.IdleState.USING)) {
                clientCnx.idleMarkTime = 0;
                return true;
            }
            if (isUsing(clientCnx)){
                return true;
            }
        }
    }

    /**
     * Changes the idle-state of the connection to #{@link ClientCnx.IdleState#RELEASING}, This method only changes this
     * connection from the #{@link ClientCnx.IdleState#IDLE} state to the #{@link ClientCnx.IdleState#RELEASING} state.
     * @return Whether change idle-stat to #{@link ClientCnx.IdleState#RELEASING} success.
     */
    public static boolean tryMarkReleasing(ClientCnx clientCnx) {
        return clientCnx.compareAndSetIdleStat(ClientCnx.IdleState.IDLE, ClientCnx.IdleState.RELEASING);
    }

    /**
     * Changes the idle-state of the connection to #{@link ClientCnx.IdleState#RELEASED}, This method only changes this
     * connection from the #{@link ClientCnx.IdleState#RELEASING} state to the #{@link ClientCnx.IdleState#RELEASED}
     * state, and close {@param clientCnx} if change state to #{@link ClientCnx.IdleState#RELEASED} success.
     * @return Whether change idle-stat to #{@link ClientCnx.IdleState#RELEASED} and close connection success.
     */
    public static boolean tryMarkReleasedAndCloseConnection(ClientCnx clientCnx) {
        if (!clientCnx.compareAndSetIdleStat(ClientCnx.IdleState.RELEASING, ClientCnx.IdleState.RELEASED)) {
            return false;
        }
        clientCnx.close();
        return true;
    }

    /**
     * Check whether the connection is idle, and if so, set the idle-state to #{@link ClientCnx.IdleState#IDLE}.
     * If the state is already idle and the {@param maxIdleSeconds} is reached, set the state to
     * #{@link ClientCnx.IdleState#RELEASING}.
     */
    public static void doIdleDetect(ClientCnx clientCnx, long maxIdleSeconds) {
        if (isReleasing(clientCnx)) {
            return;
        }
        if (isIdle(clientCnx)) {
            if (maxIdleSeconds * 1000 + clientCnx.idleMarkTime < System.currentTimeMillis()) {
                tryMarkReleasing(clientCnx);
            }
            return;
        }
        if (clientCnx.idleCheck()) {
            tryMarkIdleAndInitIdleTime(clientCnx);
        }
    }
}
