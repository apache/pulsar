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
    static final public String INCOMING = "_elasticsearch_incoming";

    // INCOMING = SUCCESS + FAILURE + SKIP + NULLVALUE_IGNORE
    static final public String SUCCESS = "_elasticsearch_success";

    // DELETE_ATTEMPT is an attempt to delete a document by id
    // TODO: add delete success metrics, currently it's difficult to separate delete and index from the bulk operations
    static final public String DELETE_ATTEMPT = "elasticsearch_delete_attempt";

    static final public String FAILURE = "elasticsearch_failure";
    static final public String SKIP = "elasticsearch_skip";
    static final public String WARN = "elasticsearch_warn";
    static final public String MALFORMED_IGNORE = "elasticsearch_malformed_ignore";
    static final public String NULLVALUE_IGNORE = "elasticsearch_nullvalue_ignore";

    public ElasticSearchMetrics(SinkContext sinkContext) {
        this.sinkContext = sinkContext;
    }

    public void incrementCounter(String counter, double value) {
        this.sinkContext.recordMetric(counter, value);
    }
}
