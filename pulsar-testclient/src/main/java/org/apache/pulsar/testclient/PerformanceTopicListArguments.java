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
package org.apache.pulsar.testclient;

import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.common.naming.TopicName;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * PerformanceTopicListArguments provides common topic list arguments which are used
 * by the consumer, producer, and reader commands, but not by the transaction test command.
 */
public abstract class PerformanceTopicListArguments extends PerformanceBaseArguments {

    @Parameters(description = "persistent://prop/ns/my-topic", arity = "1")
    public List<String> topics;

    @Option(names = { "-t", "--num-topics", "--num-topic" }, description = "Number of topics.  Must match"
            + "the given number of topic arguments.",
            converter = PositiveNumberParameterConvert.class
    )
    public int numTopics = 1;

    public PerformanceTopicListArguments(String cmdName) {
        super(cmdName);
    }

    @Override
    public void validate() throws Exception {
        super.validate();
        for (String arg : topics) {
            if (arg.startsWith("-")) {
                String errMsg = String.format("invalid option: '%s', to use a topic with the name '%s', "
                        + "please use a fully qualified topic name", arg, arg);
                throw new Exception(errMsg);
            }
        }

        if (topics.size() != numTopics) {
            // keep compatibility with the previous version
            if (topics.size() == 1) {
                String prefixTopicName = TopicName.get(topics.get(0)).toString().trim();
                List<String> defaultTopics = new ArrayList<>();
                for (int i = 0; i < numTopics; i++) {
                    defaultTopics.add(String.format("%s-%d", prefixTopicName, i));
                }
                topics = defaultTopics;
            } else {
                String errMsg = String.format("the number of topic names (%d) must be equal to --num-topics (%d)",
                        topics.size(), numTopics);
                throw new Exception(errMsg);
            }
        }
    }
}
