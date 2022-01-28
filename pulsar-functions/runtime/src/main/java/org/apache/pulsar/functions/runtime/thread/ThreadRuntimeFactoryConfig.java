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
package org.apache.pulsar.functions.runtime.thread;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.configuration.FieldContext;

@Data
@Accessors(chain = true)
public class ThreadRuntimeFactoryConfig {

    @Data
    @Accessors(chain = true)
    /**
     * Memory limit set for the pulsar client used by all instances
     * If `absoluteValue` and `percentOfMaxDirectMemory` are both set, then the min of the two will be used.
     */
    public static class MemoryLimit {
        @FieldContext(
                doc = "The max memory in bytes the pulsar client can use"
        )
        Long absoluteValue;
        @FieldContext(
                doc = "The max memory the pulsar client can use as a percentage of max direct memory set for JVM"
        )
        Double percentOfMaxDirectMemory;
    }

    @FieldContext(
        doc = "The name of thread group running function threads"
    )
    protected String threadGroupName;

    @FieldContext(
            doc = "Memory limit set for the pulsar client used by all instances"
    )
    protected MemoryLimit pulsarClientMemoryLimit;
}
