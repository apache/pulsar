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
package org.apache.pulsar.broker.service;

/**
 * Trace point names used while loading a topic.
 */
public final class TopicLoadingTracePoints {

    public static final String TOPIC_EXISTS = "topic-exists";
    public static final String LOCAL_TOPIC_POLICIES = "local-topic-policies";
    public static final String GLOBAL_TOPIC_POLICIES = "global-topic-policies";
    public static final String NAMESPACE_POLICIES = "namespace-policies";
    public static final String LOCAL_POLICIES = "local-policies";
    public static final String OWNERSHIP = "ownership";
    public static final String MAX_CONCURRENT_LOADING_LIMITATION = "max-concurrent-loading-limitation";
    public static final String PROPERTIES = "properties";
    public static final String MAX_TOPICS_PER_NAMESPACE = "max-topics-per-namespace";
    public static final String CHECK_TOPIC_ALREADY_MIGRATED = "check-topic-already-migrated";
    public static final String VALIDATE_TOPIC_CONSISTENCY = "validate-topic-consistency";
    public static final String ML_CONFIG = "ml-config";
    public static final String OPEN_ML = "open-ml";
    public static final String INIT = "init";
    public static final String PRE_CREATE_COMPACTED_SUB = "pre-create-compacted-sub";
    public static final String REPLICATION = "replication";
    public static final String DEDUPLICATION = "deduplication";

    private TopicLoadingTracePoints() {
    }
}
