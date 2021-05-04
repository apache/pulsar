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

import io.netty.util.ReferenceCounted;

public interface InvalidateableReferenceCounted extends ReferenceCounted {
    /**
     * Marks the entry to be removed. Internally {@link ReferenceCounted#release()} will be called unless
     * the entry has already been invalidated before. No calls to {@link ReferenceCounted#release()} should be
     * made separately to invalidate the entry.
     *
     * @return true if the value was marked to be invalidated, false if it was already invalidated before.
     */
    boolean invalidate();
}
