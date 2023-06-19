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
package org.apache.pulsar.io.elasticsearch;

import org.apache.pulsar.io.core.SinkContext;

/*
 * Metrics class for ElasticSearchSink
 */
public class ElasticSearchMetrics {

    private SinkContext sinkContext;
    // sink metrics
    public static final String INCOMING = "_elasticsearch_incoming";

    // INCOMING = SUCCESS + FAILURE + SKIP + NULLVALUE_IGNORE
    public static final String SUCCESS = "_elasticsearch_success";

    // DELETE_ATTEMPT is an attempt to delete a document by id
    // TODO: add delete success metrics, currently it's difficult to separate delete and index from the bulk operations
    public static final String DELETE_ATTEMPT = "elasticsearch_delete_attempt";

    public static final String FAILURE = "elasticsearch_failure";
    public static final String SKIP = "elasticsearch_skip";
    public static final String WARN = "elasticsearch_warn";
    public static final String MALFORMED_IGNORE = "elasticsearch_malformed_ignore";
    public static final String NULLVALUE_IGNORE = "elasticsearch_nullvalue_ignore";

    public ElasticSearchMetrics(SinkContext sinkContext) {
        this.sinkContext = sinkContext;
    }

    public void incrementCounter(String counter, double value) {
        this.sinkContext.recordMetric(counter, value);
    }
}
