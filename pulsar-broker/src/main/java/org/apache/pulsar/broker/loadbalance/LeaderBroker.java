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
package org.apache.pulsar.broker.loadbalance;

import com.google.common.base.Objects;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class to hold the contents of the leader election node. Facilitates serialization and deserialization of the
 * information that might be added for leader broker in the future.
 *
 *
 */
public class LeaderBroker {
    public final String serviceUrl;

    private AtomicBoolean isLeaderReady = new AtomicBoolean(false);

    // Need this default constructor for json conversion. Please do not remove this.
    public LeaderBroker() {
        this(null);
    }

    public LeaderBroker(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getServiceUrl() {
        return this.serviceUrl;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(serviceUrl);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LeaderBroker) {
            LeaderBroker other = (LeaderBroker) obj;
            return Objects.equal(serviceUrl, other.serviceUrl);
        }
        return false;
    }

    public boolean isLeaderReady() {
        return isLeaderReady.get();
    }

    public void setLeaderReady(boolean isLeaderReady) {
        this.isLeaderReady.compareAndSet(!isLeaderReady, isLeaderReady);
    }
}
