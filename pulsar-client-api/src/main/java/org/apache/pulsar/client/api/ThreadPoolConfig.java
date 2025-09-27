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
 * Configuration interface for thread pool settings.
 */
public interface ThreadPoolConfig<T extends ThreadPoolConfig<?>> {
    /**
     * Sets the name of the thread pool.
     *
     * @param name the name to set for the thread pool
     * @return this config instance for method chaining
     */
    T name(String name);

    /**
     * Sets the number of threads in the thread pool.
     *
     * @param numberOfThreads the number of threads to use
     * @return this config instance for method chaining
     */
    T numberOfThreads(int numberOfThreads);

    /**
     * Sets whether the threads should be daemon threads.
     *
     * @param daemon true if the threads should be daemon threads, false otherwise
     * @return this config instance for method chaining
     */
    T daemon(boolean daemon);
}
