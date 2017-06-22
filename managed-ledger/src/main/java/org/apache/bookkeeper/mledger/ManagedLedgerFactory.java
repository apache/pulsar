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

import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;

import com.google.common.annotations.Beta;

/**
 * A factory to open/create managed ledgers and delete them.
 *
 */
@Beta
public interface ManagedLedgerFactory {

    /**
     * Open a managed ledger. If the managed ledger does not exist, a new one will be automatically created. Uses the
     * default configuration parameters.
     *
     * @param name
     *            the unique name that identifies the managed ledger
     * @return the managed ledger
     * @throws ManagedLedgerException
     */
    public ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException;

    /**
     * Open a managed ledger. If the managed ledger does not exist, a new one will be automatically created.
     *
     * @param name
     *            the unique name that identifies the managed ledger
     * @param config
     *            managed ledger configuration
     * @return the managed ledger
     * @throws ManagedLedgerException
     */
    public ManagedLedger open(String name, ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronous open method.
     *
     * @see #open(String)
     * @param name
     *            the unique name that identifies the managed ledger
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncOpen(String name, OpenLedgerCallback callback, Object ctx);

    /**
     * Asynchronous open method.
     *
     * @see #open(String, ManagedLedgerConfig)
     * @param name
     *            the unique name that identifies the managed ledger
     * @param config
     *            managed ledger configuration
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncOpen(String name, ManagedLedgerConfig config, OpenLedgerCallback callback, Object ctx);

    /**
     * Get the current metadata info for a managed ledger
     *
     * @param name
     *            the unique name that identifies the managed ledger
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public ManagedLedgerInfo getManagedLedgerInfo(String name) throws InterruptedException, ManagedLedgerException;

    /**
     * Asynchronously get the current metadata info for a managed ledger
     *
     * @param name
     *            the unique name that identifies the managed ledger
     * @param callback
     *            callback object
     * @param ctx
     *            opaque context
     */
    public void asyncGetManagedLedgerInfo(String name, ManagedLedgerInfoCallback callback, Object ctx);

    /**
     * Releases all the resources maintained by the ManagedLedgerFactory
     *
     * @throws ManagedLedgerException
     */
    public void shutdown() throws InterruptedException, ManagedLedgerException;

}
