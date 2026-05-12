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
package org.apache.pulsar.broker.transaction.metadata;

/**
 * Transaction header states. Stored as strings in {@link TxnHeader#getState()} to keep the wire
 * format human-readable and tolerant of future state additions.
 *
 * <p>Valid transitions, applied by the v5 TC as version-conditional puts:
 * <pre>
 *   OPEN -> COMMITTED   (terminal)
 *   OPEN -> ABORTED     (terminal)
 * </pre>
 */
public final class TxnState {

    public static final String OPEN = "OPEN";
    public static final String COMMITTED = "COMMITTED";
    public static final String ABORTED = "ABORTED";

    private TxnState() {}

    /** @return {@code true} if {@code state} is one of the terminal states (COMMITTED/ABORTED). */
    public static boolean isTerminal(String state) {
        return COMMITTED.equals(state) || ABORTED.equals(state);
    }
}
