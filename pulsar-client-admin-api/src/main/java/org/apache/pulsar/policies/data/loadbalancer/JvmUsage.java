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
package org.apache.pulsar.policies.data.loadbalancer;

import java.util.Map;

/**
 * {@link JvmUsage} represents set of resources that are specific to JVM and are used by broker, load balancing need to
 * know this detail.
 *
 */
public class JvmUsage {
    public long getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(long threadCount) {
        this.threadCount = threadCount;
    }

    private Long threadCount;

    /*
     * factory method that returns a new instance of class by populating it from metrics, we assume that the metrics is
     * jvm metrics or
     *
     */
    public static JvmUsage populateFrom(Map<String, Object> metrics) {
        JvmUsage jvmUsage = null;
        if (metrics.containsKey("jvm_thread_cnt")) {
            jvmUsage = new JvmUsage();
            jvmUsage.threadCount = Long.valueOf(metrics.get("jvm_thread_cnt").toString());
        }
        return jvmUsage;
    }
}
