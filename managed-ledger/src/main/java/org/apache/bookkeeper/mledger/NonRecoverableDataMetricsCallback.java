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

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * Callback interface for reporting metrics when non-recoverable data is skipped.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface NonRecoverableDataMetricsCallback {

    /**
     * Called when a non-recoverable ledger is skipped.
     * @param ledgerId the ledger ID that was skipped
     */
    void onSkipNonRecoverableLedger(long ledgerId);

    /**
     * Called when non-recoverable entries are skipped.
     * @param entryCount the number of entries that were skipped
     */
    void onSkipNonRecoverableEntries(long entryCount);
}