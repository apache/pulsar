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

package org.apache.pulsar.client.api;

/**
 * Configuration interface for memory limit settings.
 */
public interface MemoryLimitConfig {

    /**
     * Configure a limit on the amount of direct memory that will be allocated by this shared client instance.
     * <p>
     * See also {@link ClientBuilder#memoryLimit(long, SizeUnit)}.
     *
     * @param memoryLimit the memory limit value, setting this to 0 will disable the limit
     * @param unit        the memory limit size unit
     * @return the memory limit configuration instance for chained calls
     */
    MemoryLimitConfig memoryLimit(long memoryLimit, SizeUnit unit);

}
