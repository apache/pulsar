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
package org.apache.pulsar.common.policies.data;

import java.util.Objects;
import lombok.SneakyThrows;
import lombok.ToString;

/**
 * Configuration of bookkeeper persistence policies.
 */
@ToString
public class PersistencePolicies implements Cloneable {
    private int bookkeeperEnsemble;
    private int bookkeeperWriteQuorum;
    private int bookkeeperAckQuorum;
    private double managedLedgerMaxMarkDeleteRate;
    private String managedLedgerStorageClassName;

    public PersistencePolicies() {
        this(2, 2, 2, 0.0, null);
    }

    public PersistencePolicies(int bookkeeperEnsemble, int bookkeeperWriteQuorum, int bookkeeperAckQuorum,
                               double managedLedgerMaxMarkDeleteRate) {
        this(bookkeeperEnsemble, bookkeeperWriteQuorum, bookkeeperAckQuorum, managedLedgerMaxMarkDeleteRate, null);
    }

    public PersistencePolicies(int bookkeeperEnsemble, int bookkeeperWriteQuorum, int bookkeeperAckQuorum,
            double managedLedgerMaxMarkDeleteRate, String managedLedgerStorageClassName) {
        this.bookkeeperEnsemble = bookkeeperEnsemble;
        this.bookkeeperWriteQuorum = bookkeeperWriteQuorum;
        this.bookkeeperAckQuorum = bookkeeperAckQuorum;
        this.managedLedgerMaxMarkDeleteRate = managedLedgerMaxMarkDeleteRate;
        this.managedLedgerStorageClassName = managedLedgerStorageClassName;
    }

    public int getBookkeeperEnsemble() {
        return bookkeeperEnsemble;
    }

    public int getBookkeeperWriteQuorum() {
        return bookkeeperWriteQuorum;
    }

    public int getBookkeeperAckQuorum() {
        return bookkeeperAckQuorum;
    }

    public double getManagedLedgerMaxMarkDeleteRate() {
        return managedLedgerMaxMarkDeleteRate;
    }

    public String getManagedLedgerStorageClassName() {
        return managedLedgerStorageClassName;
    }

    @SneakyThrows
    @Override
    protected PersistencePolicies clone() {
        return PersistencePolicies.class.cast(super.clone());
    }

    @Override
    public int hashCode() {
        return Objects.hash(bookkeeperEnsemble, bookkeeperWriteQuorum,
                bookkeeperAckQuorum, managedLedgerMaxMarkDeleteRate, managedLedgerStorageClassName);
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PersistencePolicies) {
            PersistencePolicies other = (PersistencePolicies) obj;
            return bookkeeperEnsemble == other.bookkeeperEnsemble
                    && bookkeeperWriteQuorum == other.bookkeeperWriteQuorum
                    && bookkeeperAckQuorum == other.bookkeeperAckQuorum
                    && managedLedgerMaxMarkDeleteRate == other.managedLedgerMaxMarkDeleteRate
                    && managedLedgerStorageClassName == other.managedLedgerStorageClassName;
        }

        return false;
    }
}
