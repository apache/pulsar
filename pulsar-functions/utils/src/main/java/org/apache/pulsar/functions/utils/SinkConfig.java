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
package org.apache.pulsar.functions.utils;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Data
@EqualsAndHashCode
@ToString
public class SinkConfig {

    private String tenant;
    private String namespace;
    private String name;
    private String className;
    private String sourceSubscriptionName;

    private Collection<String> inputs;

    private Map<String, String> topicToSerdeClassName;

    private String topicsPattern;

    private Map<String, String> topicToSchemaType;

    private Map<String, ConsumerConfig> inputSpecs = new TreeMap<>();

    private Map<String, Object> configs;
    private int parallelism = 1;
    private FunctionConfig.ProcessingGuarantees processingGuarantees;
    private boolean retainOrdering;
    private Resources resources;
    private boolean autoAck;
    private Long timeoutMs;

    private String archive;
}
