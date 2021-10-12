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
package org.apache.pulsar.functions.instance.go;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.functions.proto.Function;

@Setter
@Getter
public class GoInstanceConfig {
    private String pulsarServiceURL = "";
    private int instanceID;
    private String funcID = "";
    private String funcVersion = "";
    private int maxBufTuples;
    private int port;
    private String clusterName = "";
    private int killAfterIdleMs;
    private int expectedHealthCheckInterval;

    private String tenant = "";
    private String nameSpace = "";
    private String name = "";
    private String className = "";
    private String logTopic = "";
    private int processingGuarantees;
    private String secretsMap = "";
    private String userConfig = "";
    private int runtime;
    private boolean autoAck;
    private int parallelism;

    private int subscriptionType;
    private long timeoutMs;
    private String subscriptionName = "";
    private boolean cleanupSubscription;
    private int subscriptionPosition = Function.SubscriptionPosition.LATEST.getNumber();

    private String sourceSpecsTopic = "";
    private String sourceSchemaType = "";
    private boolean isRegexPatternSubscription;
    private int receiverQueueSize;

    private String sinkSpecsTopic = "";
    private String sinkSchemaType = "";

    private double cpu;
    private long ram;
    private long disk;

    private int maxMessageRetries;
    private String deadLetterTopic = "";

    private int metricsPort;
}
