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
package org.apache.pulsar.common.policies.data;

/**
 * Information about the namespace's ownership.
 */
public class NamespaceOwnershipStatus {

    @SuppressWarnings("checkstyle:MemberName")
    public BrokerAssignment broker_assignment;
    @SuppressWarnings("checkstyle:MemberName")
    public boolean is_controlled;
    @SuppressWarnings("checkstyle:MemberName")
    public boolean is_active;

    public NamespaceOwnershipStatus() {
        this(BrokerAssignment.shared, false, false);
    }

    public NamespaceOwnershipStatus(BrokerAssignment brokerStatus, boolean controlled, boolean active) {
        this.broker_assignment = brokerStatus;
        this.is_controlled = controlled;
        this.is_active = active;
    }

    @Override
    public String toString() {
        return String.format("[broker_assignment=%s is_controlled=%s is_active=%s]", broker_assignment, is_controlled,
                is_active);
    }
}
