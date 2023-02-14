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
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Init;
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
        ServiceUnitState prevState = from == null ? Init : from.state();
        ServiceUnitState state = to == null ? Init : to.state();
        if (!ServiceUnitState.isValidTransition(prevState, state)) {
            return true;
        }

        if (checkBrokers) {
            switch (prevState) {
                case Free:
                    switch (state) {
                        case Assigned:
                            return isNotBlank(to.sourceBroker());
                    }
                case Owned:
                    switch (state) {
                        case Assigned:
                            return invalidTransfer(from, to);
                        case Splitting:
                        case Free:
                            return !from.broker().equals(to.broker());
                    }
                case Assigned:
                    switch (state) {
                        case Released:
                            return isBlank(to.sourceBroker()) || notEquals(from, to);
                        case Owned:
                            return isNotBlank(to.sourceBroker()) || !to.broker().equals(from.broker());
                    }
                case Released:
                    switch (state) {
                        case Owned:
                            return notEquals(from, to);
                    }
                case Splitting:
                    switch (state) {
                        case Disabled :
                            return notEquals(from, to);
                    }
            }
        }
        return false;
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