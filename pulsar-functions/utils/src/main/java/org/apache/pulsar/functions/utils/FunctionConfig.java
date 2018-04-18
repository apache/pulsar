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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

@Getter
@Setter
@Data
@EqualsAndHashCode
@ToString
public class FunctionConfig {

    public enum ProcessingGuarantees {
        ATLEAST_ONCE,
        ATMOST_ONCE,
        EFFECTIVELY_ONCE
    }

    public enum SubscriptionType {
        SHARED,
        EXCLUSIVE,
        FAILOVER
    }

    public enum Runtime {
        JAVA,
        PYTHON
    }

    private String tenant;
    private String namespace;
    private String name;
    private String className;

    private Collection<String> inputs = new LinkedList<>();
    private Map<String, String> customSerdeInputs = new HashMap<>();

    private String output;
    private String outputSerdeClassName;

    private String logTopic;
    private ProcessingGuarantees processingGuarantees;
    private Map<String, String> userConfig = new HashMap<>();
    private SubscriptionType subscriptionType;
    private Runtime runtime;
    private boolean autoAck;
    private int parallelism;
    private String fqfn;
}
