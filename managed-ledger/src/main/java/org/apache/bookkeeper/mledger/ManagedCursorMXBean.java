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

import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;

/**
 * JMX Bean interface for ManagedCursor stats.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Stable
public interface ManagedCursorMXBean {

    /**
     * @return the ManagedCursor name
     */
    String getName();

    /**
     * @return the ManagedLedger name
     */
    String getLedgerName();

    /**
     * persist cursor by ledger.
     * @param success
     */
    void persistToLedger(boolean success);

    /**
     * persist cursor by zookeeper.
     * @param success
     */
    void persistToZookeeper(boolean success);

    /**
     * @return the number of persist cursor by ledger that succeed
     */
    long getPersistLedgerSucceed();

    /**
     * @return the number of persist cursor by ledger that failed
     */
    long getPersistLedgerErrors();

    /**
     * @return the number of persist cursor by zookeeper that succeed
     */
    long getPersistZookeeperSucceed();

    /**
     * @return the number of persist cursor by zookeeper that failed
     */
    long getPersistZookeeperErrors();

    /**
     * Add write data to a ledger of a cursor (in bytes).
     * This will update writeCursorLedgerLogicalSize and writeCursorLedgerSize.
     *
     * @param size Size of data written to cursor (in bytes)
     */
    void addWriteCursorLedgerSize(long size);

    /**
     * Add read data from a ledger of a cursor (in bytes).
     *
     * @param size Size of data read from cursor (in bytes)
     */
    void addReadCursorLedgerSize(long size);

    /**
     * @return the size of data written to cursor (in bytes)
     */
    long getWriteCursorLedgerSize();

    /**
     * @return the size of data written to cursor without replicas (in bytes)
     */
    long getWriteCursorLedgerLogicalSize();

    /**
     * @return the size of data read from cursor (in bytes)
     */
    long getReadCursorLedgerSize();

}
