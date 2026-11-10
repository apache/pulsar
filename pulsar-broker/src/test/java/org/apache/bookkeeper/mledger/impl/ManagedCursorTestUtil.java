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
package org.apache.bookkeeper.mledger.impl;

import org.apache.bookkeeper.mledger.ManagedCursor;

public class ManagedCursorTestUtil {
    /**
     * Sets the last active time of a cursor, e.g. to make its subscription look inactive for longer than the broker's
     * check for expired subscriptions allows, without waiting for that.
     *
     * @param cursor     the cursor, a {@link ManagedCursorImpl}
     * @param lastActive the last active time (Unix time, milliseconds)
     */
    public static void setLastActive(ManagedCursor cursor, long lastActive) {
        ((ManagedCursorImpl) cursor).setLastActive(lastActive);
    }
}
