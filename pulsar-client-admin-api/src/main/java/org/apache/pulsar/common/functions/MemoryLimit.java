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
package org.apache.pulsar.common.functions;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Memory limit of the Pulsar client used by a function instance.
 * If both {@code limitInBytes} and {@code percentOfMaxDirectMemory} are set,
 * the smaller of the two limits is used.
 */
@Data
@Accessors(chain = true)
public class MemoryLimit {
    // The max memory in bytes the Pulsar client can use
    private Long limitInBytes;
    // The max memory the Pulsar client can use, as a percentage (0, 100] of the JVM max direct memory
    private Double percentOfMaxDirectMemory;
}
