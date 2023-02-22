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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData.state;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
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
        if (to == null) {
            return false;
        } else if (to.force()) {
            return false;
        }


        ServiceUnitState prevState = state(from);
        ServiceUnitState state = state(to);

        if (!ServiceUnitState.isValidTransition(prevState, state)) {
            return true;
        }

        if (checkBrokers) {
            switch (prevState) {
                case Owned:
                    switch (state) {
                        case Assigning:
                            return invalidTransfer(from, to);
                        case Splitting:
                        case Releasing:
                            return isNotBlank(to.sourceBroker()) || targetNotEquals(from, to);
                    }
                case Assigning:
                    switch (state) {
                        case Releasing:
                            return isBlank(to.sourceBroker()) || notEquals(from, to);
                        case Owned:
                            return isNotBlank(to.sourceBroker()) || targetNotEquals(from, to);
                    }
                case Releasing:
                    switch (state) {
                        case Owned:
                        case Free:
                            return notEquals(from, to);
                    }
                case Splitting:
                    switch (state) {
                        case Deleted:
                            return notEquals(from, to);
                    }
                case Free:
                    switch (state) {
                        case Assigning:
                            return isNotBlank(to.sourceBroker());
                    }
            }
        }
        return false;
    }

    private boolean targetNotEquals(ServiceUnitStateData from, ServiceUnitStateData to) {
        return !from.broker().equals(to.broker());
    }

    private boolean notEquals(ServiceUnitStateData from, ServiceUnitStateData to) {
        return !from.broker().equals(to.broker())
                || !StringUtils.equals(from.sourceBroker(), to.sourceBroker());
    }

    private boolean invalidTransfer(ServiceUnitStateData from, ServiceUnitStateData to) {
        return !from.broker().equals(to.sourceBroker())
                || from.broker().equals(to.broker());
    }
}