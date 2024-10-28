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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/**
 * Defines data for the service unit state changes.
 * This data will be broadcast in ServiceUnitStateChannel.
 */

public record ServiceUnitStateData(
        ServiceUnitState state, String dstBroker, String sourceBroker,
        Map<String, Optional<String>> splitServiceUnitToDestBroker, boolean force, long timestamp, long versionId) {

    public ServiceUnitStateData {
        Objects.requireNonNull(state);
        if (state != ServiceUnitState.Free && StringUtils.isBlank(dstBroker) && StringUtils.isBlank(sourceBroker)) {
            throw new IllegalArgumentException("Empty broker");
        }
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker,
                                Map<String, Optional<String>> splitServiceUnitToDestBroker, long versionId) {
        this(state, dstBroker, sourceBroker, splitServiceUnitToDestBroker, false,
                System.currentTimeMillis(), versionId);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker,
                                Map<String, Optional<String>> splitServiceUnitToDestBroker, boolean force,
                                long versionId) {
        this(state, dstBroker, sourceBroker, splitServiceUnitToDestBroker, force,
                System.currentTimeMillis(), versionId);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker, long versionId) {
        this(state, dstBroker, sourceBroker, null, false, System.currentTimeMillis(), versionId);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, String sourceBroker, boolean force,
                                long versionId) {
        this(state, dstBroker, sourceBroker, null, force,
                System.currentTimeMillis(), versionId);
    }



    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, long versionId) {
        this(state, dstBroker, null, null, false, System.currentTimeMillis(), versionId);
    }

    public ServiceUnitStateData(ServiceUnitState state, String dstBroker, boolean force, long versionId) {
        this(state, dstBroker, null, null, force, System.currentTimeMillis(), versionId);
    }

    public static ServiceUnitState state(ServiceUnitStateData data) {
        return data == null ? ServiceUnitState.Init : data.state();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServiceUnitStateData that = (ServiceUnitStateData) o;

        return versionId == that.versionId;
    }
}
