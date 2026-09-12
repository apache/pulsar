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
package org.apache.pulsar.functions.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.functions.proto.FunctionState;

public class FunctionMetaDataUtils {

    public static boolean canChangeState(FunctionMetaData functionMetaData, int instanceId,
                                         FunctionState newState) {
        if (instanceId >= functionMetaData.getFunctionDetails().getParallelism()) {
            return false;
        }
        if (functionMetaData.getInstanceStatesCount() == 0) {
            // This means that all instances of the functions are running
            return newState == FunctionState.STOPPED;
        }
        if (instanceId >= 0) {
            try {
                FunctionState currentState = functionMetaData.getInstanceStates(instanceId);
                return currentState != newState;
            } catch (IllegalArgumentException e) {
                return false;
            }
        } else {
            // want to change state for all instances
            AtomicBoolean canChange = new AtomicBoolean(false);
            functionMetaData.forEachInstanceStates((id, state) -> {
                if (state != newState) {
                    canChange.set(true);
                }
            });
            return canChange.get();
        }
    }

    public static FunctionMetaData changeFunctionInstanceStatus(FunctionMetaData functionMetaData,
                                                                         Integer instanceId, boolean start) {
        FunctionMetaData result = new FunctionMetaData().copyFrom(functionMetaData)
                .setVersion(functionMetaData.getVersion() + 1);
        if (result.getInstanceStatesCount() == 0) {
            for (int i = 0; i < functionMetaData.getFunctionDetails().getParallelism(); ++i) {
                result.putInstanceStates(i, FunctionState.RUNNING);
            }
        }
        FunctionState state = start ? FunctionState.RUNNING : FunctionState.STOPPED;
        if (instanceId < 0) {
            for (int i = 0; i < functionMetaData.getFunctionDetails().getParallelism(); ++i) {
                result.putInstanceStates(i, state);
            }
        } else if (instanceId < result.getFunctionDetails().getParallelism()) {
            result.putInstanceStates(instanceId, state);
        }
        return result;
    }

    public static FunctionMetaData incrMetadataVersion(FunctionMetaData existingMetaData,
                                                                FunctionMetaData updatedMetaData) {
        long version = 0;
        if (existingMetaData != null) {
            version = existingMetaData.getVersion() + 1;
        }
        return new FunctionMetaData().copyFrom(updatedMetaData)
                .setVersion(version);
    }
}
