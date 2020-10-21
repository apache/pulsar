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

package org.apache.pulsar.functions.utils;

import org.apache.pulsar.functions.proto.Function;

public class FunctionMetaDataUtils {

    public static boolean canChangeState(Function.FunctionMetaData functionMetaData, int instanceId, Function.FunctionState newState) {
        if (instanceId >= functionMetaData.getFunctionDetails().getParallelism()) {
            return false;
        }
        if (functionMetaData.getInstanceStatesMap() == null || functionMetaData.getInstanceStatesMap().isEmpty()) {
            // This means that all instances of the functions are running
            return newState == Function.FunctionState.STOPPED;
        }
        if (instanceId >= 0) {
            if (functionMetaData.getInstanceStatesMap().containsKey(instanceId)) {
                return functionMetaData.getInstanceStatesMap().get(instanceId) != newState;
            } else {
                return false;
            }
        } else {
            // want to change state for all instances
            for (Function.FunctionState state : functionMetaData.getInstanceStatesMap().values()) {
                if (state != newState) return true;
            }
            return false;
        }
    }

    public static Function.FunctionMetaData changeFunctionInstanceStatus(Function.FunctionMetaData functionMetaData,
                                                                         Integer instanceId, boolean start) {
        Function.FunctionMetaData.Builder builder = functionMetaData.toBuilder()
                .setVersion(functionMetaData.getVersion() + 1);
        if (builder.getInstanceStatesMap() == null || builder.getInstanceStatesMap().isEmpty()) {
            for (int i = 0; i < functionMetaData.getFunctionDetails().getParallelism(); ++i) {
                builder.putInstanceStates(i, Function.FunctionState.RUNNING);
            }
        }
        Function.FunctionState state = start ? Function.FunctionState.RUNNING : Function.FunctionState.STOPPED;
        if (instanceId < 0) {
            for (int i = 0; i < functionMetaData.getFunctionDetails().getParallelism(); ++i) {
                builder.putInstanceStates(i, state);
            }
        } else if (instanceId < builder.getFunctionDetails().getParallelism()){
            builder.putInstanceStates(instanceId, state);
        }
        return builder.build();
    }

    public static Function.FunctionMetaData incrMetadataVersion(Function.FunctionMetaData existingMetaData,
                                                                Function.FunctionMetaData updatedMetaData) {
        long version = 0;
        if (existingMetaData != null) {
            version = existingMetaData.getVersion() + 1;
        }
        return updatedMetaData.toBuilder()
                .setVersion(version)
                .build();
    }
}