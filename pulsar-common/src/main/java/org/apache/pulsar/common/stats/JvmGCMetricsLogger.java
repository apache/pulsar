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
package org.apache.pulsar.common.stats;

/**
 *
 * {@link JvmGCMetricsLogger} can be implemented for each specific GC type which retrieves GC count and pause time and
 * logs it into metrics.
 *
 */
public interface JvmGCMetricsLogger {

    /**
     * {@link JvmGCMetricsLogger} should update the metrics with GC specific dimensions and value.
     *
     * @param metrics
     */
    void logMetrics(Metrics metrics);

    /**
     * It will be triggered by {@link JvmMetrics} periodically to refresh stats at interval (default = 1 min).
     */
    void refresh();
}
