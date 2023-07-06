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
package org.apache.pulsar.common.util;

public enum Step {
    first_subscribe_end,
    received_unload1,
    second_subscribe_due_to_unload1_started,
    subscribe_timeout_triggered,
    start_unload2,
    before_reconnect_due_to_unload2,
    stop_subscribe_timeout,
    before_reconnect_due_to_timeout1,
    before_reconnect_due_to_timeout2,
    before_reconnect_due_to_unload2_started,
    before_reconnect_due_to_unload2_is_success,
    reconnect_due_to_timeout_started,
    reconnect_due_to_timeout_is_success
    ;

    public Step previousStep() {
        Step lastStep = null;
        for (Step step : Step.values()) {
            if (step.equals(this)) {
                break;
            }
            lastStep = step;
        }
        return lastStep;
    }
}
