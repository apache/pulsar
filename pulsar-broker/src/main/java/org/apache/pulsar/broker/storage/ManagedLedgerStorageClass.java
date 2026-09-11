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
package org.apache.pulsar.broker.storage;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * ManagedLedgerStorageClass represents a configured instance of ManagedLedgerFactory for managed ledgers.
 * The {@link ManagedLedgerStorage} can hold multiple storage classes, and each storage class can have its own
 * configuration.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ManagedLedgerStorageClass {
    /**
     * Return the name of the storage class.
     *
     * @return the name of the storage class.
     */
    String getName();
    /**
     * Return the factory to create {@link ManagedLedgerFactory}.
     *
     * @return the factory to create {@link ManagedLedgerFactory}.
     */
    ManagedLedgerFactory getManagedLedgerFactory();
}
