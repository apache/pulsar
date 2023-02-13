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

import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Balanced;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Unknown;
import lombok.Data;

/**
 * Defines the information required for a service unit split(e.g. bundle split).
 */
@Data
public class SplitDecision {
    Split split;
    Label label;
    Reason reason;

    public enum Label {
        Success,
        Skip,
        Failure
    }

    public enum Reason {
        Topics,
        Sessions,
        MsgRate,
        Bandwidth,
        Admin,
        Balanced,
        Unknown
    }

    public SplitDecision() {
        split = null;
        label = null;
        reason = null;
    }

    public void clear() {
        split = null;
        label = null;
        reason = null;
    }

    public void skip() {
        label = Skip;
        reason = Balanced;
    }

    public void succeed(Reason reason) {
        label = Success;
        this.reason = reason;
    }


    public void fail() {
        label = Failure;
        reason = Unknown;
    }

}