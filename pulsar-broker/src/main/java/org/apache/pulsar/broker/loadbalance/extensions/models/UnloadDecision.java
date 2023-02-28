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
package org.apache.pulsar.broker.loadbalance.extensions.models;

import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Balanced;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Data;

/**
 * Defines the information required to unload or transfer a service unit(e.g. bundle).
 */
@Data
public class UnloadDecision {
    Multimap<String, Unload> unloads;
    Label label;
    Reason reason;
    Double loadAvg;
    Double loadStd;
    public enum Label {
        Success,
        Skip,
        Failure
    }
    public enum Reason {
        Overloaded,
        Underloaded,
        Balanced,
        NoBundles,
        CoolDown,
        OutDatedData,
        NoLoadData,
        NoBrokers,
        Unknown
    }

    public UnloadDecision() {
        unloads = ArrayListMultimap.create();
        label = null;
        reason = null;
        loadAvg = null;
        loadStd = null;
    }

    public void clear() {
        unloads.clear();
        label = null;
        reason = null;
        loadAvg = null;
        loadStd = null;
    }

    public void skip(int numOfOverloadedBrokers,
                     int numOfUnderloadedBrokers,
                     int numOfBrokersWithEmptyLoadData,
                     int numOfBrokersWithFewBundles) {
        label = Skip;
        if (numOfOverloadedBrokers == 0 && numOfUnderloadedBrokers == 0) {
            reason = Balanced;
        } else if (numOfBrokersWithEmptyLoadData > 0) {
            reason = NoLoadData;
        } else if (numOfBrokersWithFewBundles > 0) {
            reason = NoBundles;
        } else {
            reason = Unknown;
        }
    }

    public void skip(Reason reason) {
        label = Skip;
        this.reason = reason;
    }

    public void succeed(
                        int numOfOverloadedBrokers,
                        int numOfUnderloadedBrokers) {

        label = Success;
        if (numOfOverloadedBrokers > numOfUnderloadedBrokers) {
            reason = Overloaded;
        } else {
            reason = Underloaded;
        }
    }


    public void fail() {
        label = Failure;
        reason = Unknown;
    }

}