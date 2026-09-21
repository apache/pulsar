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
package org.apache.pulsar.broker.service;

/**
 * A wrapper class for a Consumer instance that provides custom implementations
 * of equals and hashCode methods. The equals method returns true if and only if
 * the compared instance is the same instance.
 *
 * <p>The reason for this class is the custom implementation of {@link Consumer#equals(Object)}.
 * Using this wrapper class will be useful in use cases where it's necessary to match a key
 * in a map by instance or a value in a set by instance.</p>
 */
class ConsumerIdentityWrapper {
    final Consumer consumer;

    public ConsumerIdentityWrapper(Consumer consumer) {
        this.consumer = consumer;
    }

    /**
     * Compares this wrapper to the specified object. The result is true if and only if
     * the argument is not null and is a ConsumerIdentityWrapper object that wraps
     * the same Consumer instance.
     *
     * @param obj the object to compare this ConsumerIdentityWrapper against
     * @return true if the given object represents a ConsumerIdentityWrapper
     *         equivalent to this wrapper, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConsumerIdentityWrapper) {
            ConsumerIdentityWrapper other = (ConsumerIdentityWrapper) obj;
            return consumer == other.consumer;
        }
        return false;
    }

    /**
     * Returns a hash code for this wrapper. The hash code is computed based on
     * the wrapped Consumer instance.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return consumer.hashCode();
    }

    @Override
    public String toString() {
        return consumer.toString();
    }
}