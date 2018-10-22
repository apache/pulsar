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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

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
@JsonIgnoreProperties(ignoreUnknown = true)
public class FunctionConfig {

    public enum ProcessingGuarantees {
        ATLEAST_ONCE,
        ATMOST_ONCE,
        EFFECTIVELY_ONCE
    }

    public enum Runtime {
        JAVA,
        PYTHON
    }

    private String tenant;
    private String namespace;
    private String name;
    private String className;
    private Collection<String> inputs;
    private Map<String, String> customSerdeInputs;
    private String topicsPattern;
    private Map<String, String> customSchemaInputs;

    /**
     * A generalized way of specifying inputs
     */
    private Map<String, ConsumerConfig> inputSpecs = new TreeMap<>();

    private String output;

    /**
     * Represents either a builtin schema type (eg: 'avro', 'json', ect) or the class name for a Schema
     * implementation
     */
    private String outputSchemaType;

    private String outputSerdeClassName;
    private String logTopic;
    private ProcessingGuarantees processingGuarantees;
    private boolean retainOrdering;
    private Map<String, Object> userConfig;
    private Runtime runtime;
    private boolean autoAck;
    private int maxMessageRetries = -1;
    private String deadLetterTopic;
    private String subName;
    private int parallelism = 1;
    private Resources resources;
    private String fqfn;
    private WindowConfig windowConfig;
    private Long timeoutMs;
    private String jar;
    private String py;
}
