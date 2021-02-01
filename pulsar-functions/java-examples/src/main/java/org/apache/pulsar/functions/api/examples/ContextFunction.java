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
package org.apache.pulsar.functions.api.examples;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.util.stream.Collectors;

public class ContextFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) {
        Logger LOG = context.getLogger();
        String inputTopics = context.getInputTopics().stream().collect(Collectors.joining(", "));
        String functionName = context.getFunctionName();

        String logMessage = String.format("A message with a value of \"%s\" has arrived on one of " +
                "the following topics: %s %n", input, inputTopics);

        LOG.info(logMessage);

        String metricName = String.format("function-%s-messages-received", functionName);
        context.recordMetric(metricName, 1);

        return null;
    }
}
