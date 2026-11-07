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
package org.apache.bookkeeper.mledger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ReadOnlyManagedLedger {

    void asyncReadEntry(Position position, AsyncCallbacks.ReadEntryCallback callback, Object ctx);

    long getNumberOfEntries();

    ReadOnlyCursor createReadOnlyCursor(Position position);

    Map<String, String> getProperties();

    /**
     * Close this read-only managed ledger asynchronously.
     *
     * <p>The default implementation is a no-op for backward compatibility. Implementations that own resources should
     * override this method and release them without blocking the calling thread.
     *
     * @return a future that completes when the managed-ledger close operation finishes; cleanup started by the close
     *         operation may finish asynchronously
     */
    default CompletableFuture<Void> asyncClose() {
        return CompletableFuture.completedFuture(null);
    }

}
