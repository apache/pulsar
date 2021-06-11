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
package org.apache.pulsar.tests.integration.functions.utils;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;

@Getter
@Setter
@ToString
public class CommandGenerator {
    private static final long MB = 1048576L;
    private static final long JAVA_RUNTIME_RAM_BYTES = 128 * MB;

    public enum Runtime {
        JAVA,
        PYTHON,
        GO,
    };
    private String functionName;
    private String tenant = "public";
    private String namespace = "default";
    private String functionClassName;
    private String sourceTopic;
    private String sourceTopicPattern;
    private Map<String, String> customSereSourceTopics;
    private String sinkTopic;
    private String logTopic;
    private String outputSerDe;
    private String processingGuarantees;
    private Runtime runtime;
    private Integer parallelism;
    private String adminUrl;
    private String batchBuilder;
    private Integer windowLengthCount;
    private Long windowLengthDurationMs;
    private Integer slidingIntervalCount;
    private Long slidingIntervalDurationMs;
    private String customSchemaInputs;
    private String schemaType;
    private SubscriptionInitialPosition subscriptionInitialPosition;

    private Map<String, String> userConfig = new HashMap<>();
    public static final String JAVAJAR = "/pulsar/examples/java-test-functions.jar";
    public static final String PYTHONBASE = "/pulsar/examples/python-examples/";
    public static final String GOBASE = "/pulsar/examples/go-examples/";

    public static CommandGenerator createDefaultGenerator(String sourceTopic, String functionClassName) {
        CommandGenerator generator = new CommandGenerator();
        generator.setSourceTopic(sourceTopic);
        generator.setFunctionClassName(functionClassName);
        generator.setRuntime(Runtime.JAVA);
        return generator;
    }

    public static CommandGenerator createTopicPatternGenerator(String sourceTopicPattern, String functionClassName) {
        CommandGenerator generator = new CommandGenerator();
        generator.setSourceTopicPattern(sourceTopicPattern);
        generator.setFunctionClassName(functionClassName);
        generator.setRuntime(Runtime.JAVA);
        return generator;
    }

    public String generateLocalRunCommand(String codeFile) {
        StringBuilder commandBuilder = new StringBuilder(PulsarCluster.ADMIN_SCRIPT);
        commandBuilder.append(" functions localrun");
        if (adminUrl != null) {
            commandBuilder.append(" --broker-service-url " + adminUrl);
        }
        if (tenant != null) {
            commandBuilder.append(" --tenant " + tenant);
        }
        if (namespace != null) {
            commandBuilder.append(" --namespace " + namespace);
        }
        if (functionName != null) {
            commandBuilder.append(" --name " + functionName);
        }
        if(runtime != Runtime.GO){
            commandBuilder.append(" --className " + functionClassName);
        }
        if (StringUtils.isNotEmpty(sourceTopic)) {
            commandBuilder.append(" --inputs " + sourceTopic);
        }
        if (sinkTopic != null) {
            commandBuilder.append(" --output " + sinkTopic);
        }
        if (customSchemaInputs != null) {
            commandBuilder.append(" --custom-schema-inputs \'" + customSchemaInputs + "\'");
        }
        if (schemaType != null) {
            commandBuilder.append(" --schema-type " + schemaType);
        }
        if (subscriptionInitialPosition != null) {
            commandBuilder.append(" --subs-position " + subscriptionInitialPosition.name());
        }
        switch (runtime){
            case JAVA:
                commandBuilder.append(" --jar " + JAVAJAR);
                break;
            case PYTHON:
                if (codeFile != null) {
                    commandBuilder.append(" --py " + PYTHONBASE + codeFile);
                } else {
                    commandBuilder.append(" --py " + PYTHONBASE);
                }
                break;
            case GO:
                if (codeFile != null) {
                    commandBuilder.append(" --go " + GOBASE + codeFile);
                } else {
                    commandBuilder.append(" --go " + GOBASE);
                }
                break;
        }
        return commandBuilder.toString();
    }

    public String generateCreateFunctionCommand() {
        return generateCreateFunctionCommand(null);
    }

    public String generateCreateFunctionCommand(String codeFile) {
        StringBuilder commandBuilder = new StringBuilder(PulsarCluster.ADMIN_SCRIPT);
        if (adminUrl != null) {
            commandBuilder.append(" --admin-url ");
            commandBuilder.append(adminUrl);
        }
        commandBuilder.append(" functions create");
        if (tenant != null) {
            commandBuilder.append(" --tenant " + tenant);
        }
        if (namespace != null) {
            commandBuilder.append(" --namespace " + namespace);
        }
        if (functionName != null) {
            commandBuilder.append(" --name " + functionName);
        }
        if (runtime != Runtime.GO){
            commandBuilder.append(" --className " + functionClassName);
        }
        if (StringUtils.isNotEmpty(sourceTopic)) {
            commandBuilder.append(" --inputs " + sourceTopic);
        }
        if (sourceTopicPattern != null) {
            commandBuilder.append(" --topics-pattern " + sourceTopicPattern);
        }
        if (logTopic != null) {
            commandBuilder.append(" --logTopic " + logTopic);
        }
        if (batchBuilder != null) {
            commandBuilder.append("--batch-builder" + batchBuilder);
        }
        if (customSereSourceTopics != null && !customSereSourceTopics.isEmpty()) {
            commandBuilder.append(" --customSerdeInputs \'" + new Gson().toJson(customSereSourceTopics) + "\'");
        }
        if (sinkTopic != null) {
            commandBuilder.append(" --output " + sinkTopic);
        }
        if (outputSerDe != null) {
            commandBuilder.append(" --outputSerdeClassName " + outputSerDe);
        }
        if (processingGuarantees != null) {
            commandBuilder.append(" --processingGuarantees " + processingGuarantees);
        }
        if (!userConfig.isEmpty()) {
            commandBuilder.append(" --userConfig \'" + new Gson().toJson(userConfig) + "\'");
        }
        if (parallelism != null) {
            commandBuilder.append(" --parallelism " + parallelism);
        }
        if (windowLengthCount != null) {
            commandBuilder.append(" --windowLengthCount " + windowLengthCount);
        }
        if (windowLengthDurationMs != null)  {
            commandBuilder.append(" --windowLengthDurationMs " + windowLengthDurationMs);
        }
        if (slidingIntervalCount != null)  {
            commandBuilder.append( " --slidingIntervalCount " + slidingIntervalCount);
        }
        if (slidingIntervalDurationMs != null)  {
            commandBuilder.append(" --slidingIntervalDurationMs " + slidingIntervalDurationMs);
        }
        if (customSchemaInputs != null) {
            commandBuilder.append(" --custom-schema-inputs \'" + customSchemaInputs + "\'");
        }
        if (schemaType != null) {
            commandBuilder.append(" --schema-type " + schemaType);
        }
        if (subscriptionInitialPosition != null) {
            commandBuilder.append(" --subs-position " + subscriptionInitialPosition.name());
        }

        switch (runtime){
            case JAVA:
                commandBuilder.append(" --jar " + JAVAJAR);
                commandBuilder.append(" --ram " + JAVA_RUNTIME_RAM_BYTES);
                break;
            case PYTHON:
                if (codeFile != null) {
                    commandBuilder.append(" --py " + PYTHONBASE + codeFile);
                } else {
                    commandBuilder.append(" --py " + PYTHONBASE);
                }
                break;
            case GO:
                if (codeFile != null) {
                    commandBuilder.append(" --go " + GOBASE + codeFile);
                } else {
                    commandBuilder.append(" --go " + GOBASE);
                }
                break;
        }
        return commandBuilder.toString();
    }

    public String generateUpdateFunctionCommand() {
        return generateUpdateFunctionCommand(null);
    }

    public String generateUpdateFunctionCommand(String codeFile) {
        StringBuilder commandBuilder = new StringBuilder();
        if (adminUrl == null) {
            commandBuilder.append("/pulsar/bin/pulsar-admin functions update");
        } else {
            commandBuilder.append("/pulsar/bin/pulsar-admin");
            commandBuilder.append(" --admin-url ");
            commandBuilder.append(adminUrl);
            commandBuilder.append(" functions update");
        }
        if (tenant != null) {
            commandBuilder.append(" --tenant " + tenant);
        }
        if (namespace != null) {
            commandBuilder.append(" --namespace " + namespace);
        }
        if (functionName != null) {
            commandBuilder.append(" --name " + functionName);
        }
        if (functionClassName != null) {
            commandBuilder.append(" --className " + functionClassName);
        }
        if (StringUtils.isNotEmpty(sourceTopic)) {
            commandBuilder.append(" --inputs " + sourceTopic);
        }
        if (customSereSourceTopics != null && !customSereSourceTopics.isEmpty()) {
            commandBuilder.append(" --customSerdeInputs \'" + new Gson().toJson(customSereSourceTopics) + "\'");
        }
        if (batchBuilder != null) {
            commandBuilder.append("--batch-builder" + batchBuilder);
        }
        if (sinkTopic != null) {
            commandBuilder.append(" --output " + sinkTopic);
        }
        if (logTopic != null) {
            commandBuilder.append(" --logTopic " + logTopic);
        }
        if (outputSerDe != null) {
            commandBuilder.append(" --outputSerdeClassName " + outputSerDe);
        }
        if (processingGuarantees != null) {
            commandBuilder.append(" --processingGuarantees " + processingGuarantees);
        }
        if (!userConfig.isEmpty()) {
            commandBuilder.append(" --userConfig \'" + new Gson().toJson(userConfig) + "\'");
        }
        if (parallelism != null) {
            commandBuilder.append(" --parallelism " + parallelism);
        }
        if (windowLengthCount != null) {
            commandBuilder.append(" --windowLengthCount " + windowLengthCount);
        }
        if (windowLengthDurationMs != null)  {
            commandBuilder.append(" --windowLengthDurationMs " + windowLengthDurationMs);
        }
        if (slidingIntervalCount != null)  {
            commandBuilder.append(" --slidingIntervalCount " + slidingIntervalCount);
        }
        if (slidingIntervalDurationMs != null)  {
            commandBuilder.append(" --slidingIntervalDurationMs " + slidingIntervalDurationMs);
        }
        if (customSchemaInputs != null) {
            commandBuilder.append(" --custom-schema-inputs \'" + customSchemaInputs + "\'");
        }
        if (schemaType != null) {
            commandBuilder.append(" --schema-type " + schemaType);
        }
        if (subscriptionInitialPosition != null) {
            commandBuilder.append(" --subs-position " + subscriptionInitialPosition.name());
        }

        if (codeFile != null) {
            switch (runtime) {
                case JAVA:
                    commandBuilder.append(" --jar " + JAVAJAR);
                    commandBuilder.append(" --ram " + JAVA_RUNTIME_RAM_BYTES);
                    break;
                case PYTHON:
                    if (codeFile != null) {
                        commandBuilder.append(" --py " + PYTHONBASE + codeFile);
                    } else {
                        commandBuilder.append(" --py " + PYTHONBASE);
                    }
                    break;
                case GO:
                    if (codeFile != null) {
                        commandBuilder.append(" --go " + GOBASE + codeFile);
                    } else {
                        commandBuilder.append(" --go " + GOBASE);
                    }
                    break;
            }
        }
        return commandBuilder.toString();
    }

    public String genereateDeleteFunctionCommand() {
        StringBuilder commandBuilder = new StringBuilder("/pulsar/bin/pulsar-admin functions delete");
        if (tenant != null) {
            commandBuilder.append(" --tenant " + tenant);
        }
        if (namespace != null) {
            commandBuilder.append(" --namespace " + namespace);
        }
        if (functionName != null) {
            commandBuilder.append(" --name " + functionName);
        }
        return commandBuilder.toString();
    }

    public String generateTriggerFunctionCommand(String triggerValue) {
        StringBuilder commandBuilder = new StringBuilder("/pulsar/bin/pulsar-admin functions trigger");
        if (tenant != null) {
            commandBuilder.append(" --tenant " + tenant);
        }
        if (namespace != null) {
            commandBuilder.append(" --namespace " + namespace);
        }
        if (functionName != null) {
            commandBuilder.append(" --name " + functionName);
        }
        commandBuilder.append(" --triggerValue " + triggerValue);
        return commandBuilder.toString();
    }
}
