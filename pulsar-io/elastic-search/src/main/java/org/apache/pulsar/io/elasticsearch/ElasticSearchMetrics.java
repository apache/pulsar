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
package org.apache.pulsar.io.elasticsearch;

import org.apache.pulsar.io.core.SinkContext;

/*
 * Metrics class for ElasticSearchSink
 */
public class ElasticSearchMetrics {

    private SinkContext sinkContext;
    // sink metrics
    public static final String INCOMING = "_elasticsearch_incoming_";

    // INCOMING = SUCCESS + FAILURE + SKIP + NULLVALUE_IGNORE
    public static final String SUCCESS = "_elasticsearch_success_";

    // DELETE is an attempt to delete a document by id
    public static final String DELETE = "elasticsearch_delete_";
    public static final String FAILURE = "elasticsearch_failure_";
    public static final String SKIP = "elasticsearch_skip_";
    public static final String WARN = "elasticsearch_warn_";
    public static final String MALFORMED_IGNORE = "elasticsearch_malformed_ignore_";
    public static final String NULLVALUE_IGNORE = "elasticsearch_nullvalue_ignore_";

    public ElasticSearchMetrics(SinkContext sinkContext) {
        this.sinkContext = sinkContext;
    }

    public void incrementCounter(String counter, double value) {
        if (sinkContext != null) {
            this.sinkContext.recordMetric(counter, value);
        }
    }
}
