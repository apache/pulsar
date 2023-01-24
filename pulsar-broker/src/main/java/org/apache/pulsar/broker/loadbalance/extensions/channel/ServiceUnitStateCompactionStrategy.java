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

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Released;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;

public class ServiceUnitStateCompactionStrategy implements TopicCompactionStrategy<ServiceUnitStateData> {

    private final Schema<ServiceUnitStateData> schema;

    private boolean checkBrokers = true;

    public ServiceUnitStateCompactionStrategy() {
        schema = Schema.JSON(ServiceUnitStateData.class);
    }

    @Override
    public Schema<ServiceUnitStateData> getSchema() {
        return schema;
    }

    @VisibleForTesting
    public void checkBrokers(boolean check) {
        this.checkBrokers = check;
    }

    @Override
    public boolean shouldKeepLeft(ServiceUnitStateData from, ServiceUnitStateData to) {
        ServiceUnitState prevState = from == null ? Free : from.state();
        ServiceUnitState state = to == null ? Free : to.state();
        if (!ServiceUnitState.isValidTransition(prevState, state)) {
            return true;
        }

        if (checkBrokers) {
            if (prevState == Free && (state == Assigned || state == Owned)) {
                // Free -> Assigned || Owned broker check
                return StringUtils.isBlank(to.broker());
            } else if (prevState == Owned && state == Assigned) {
                // Owned -> Assigned(transfer) broker check
                return !StringUtils.equals(from.broker(), to.sourceBroker())
                        || StringUtils.isBlank(to.broker())
                        || StringUtils.equals(from.broker(), to.broker());
            } else if (prevState == Assigned && state == Released) {
                // Assigned -> Released(transfer) broker check
                return !StringUtils.equals(from.broker(), to.broker())
                        || !StringUtils.equals(from.sourceBroker(), to.sourceBroker());
            } else if (prevState == Released && state == Owned) {
                // Released -> Owned(transfer) broker check
                return !StringUtils.equals(from.broker(), to.broker())
                        || !StringUtils.equals(from.sourceBroker(), to.sourceBroker());
            } else if (prevState == Assigned && state == Owned) {
                // Assigned -> Owned broker check
                return !StringUtils.equals(from.broker(), to.broker())
                        || !StringUtils.equals(from.sourceBroker(), to.sourceBroker());
            } else if (prevState == Owned && state == Splitting) {
                // Owned -> Splitting broker check
                return !StringUtils.equals(from.broker(), to.broker());
            }
        }

        return false;
    }

}
