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
package org.apache.pulsar.common.tieredStorage;

/**
 * Types of supported schema for Pulsar messages
 *
 *
 * <p>Ideally we should have just one single set of enum definitions
 * for schema type. but we have 3 locations of defining schema types.
 *
 * <p>when you are adding a new schema type that whose
 * schema info is required to be recorded in schema registry,
 * add corresponding schema type into `pulsar-common/src/main/proto/PulsarApi.proto`
 * and `pulsar-broker/src/main/proto/SchemaRegistryFormat.proto`.
 */
public enum OffloadType {
    /**
     * No offload type defined
     */
    NONE(0),

    /**
     * S3 type
     */
    S3(1);

    int value;

    OffloadType(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    public static OffloadType valueOf(int value) {
        switch (value) {
            case 0: return NONE;
            case 1: return S3;
            default: return NONE;
        }
    }
}
