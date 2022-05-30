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
package org.apache.bookkeeper.mledger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;

public interface ManagedTrash {

    String MANAGED_LEDGER = "managed-ledger";

    String MANAGED_CURSOR = "managed-cursor";

    String SCHEMA = "schema";

    String DELETABLE_LEDGER_SUFFIX = "DL";

    String DELETABLE_OFFLOADED_LEDGER_SUFFIX = "DOL";

    String name();

    CompletableFuture<?> initialize();

    CompletableFuture<?> appendLedgerTrashData(long ledgerId, LedgerInfo context, String type);

    CompletableFuture<?> asyncUpdateTrashData();

    void triggerDelete();

    List<Long> getAllArchiveIndex();

    Map<String, LedgerInfo> getArchiveData(long index);

    long getTrashDataSize();

    long getToArchiveDataSize();

    void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx);
}
