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
package org.apache.pulsar.io.batchdatagenerator;

import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.io.batchdiscovery.CronTriggerer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Useful for testing within IDE.
 *
 */
public class BatchDataGeneratorExec {

     public static void main(final String[] args) throws Exception {

         final String cronString = "0 0/5 * * * ?";
         final Map<String, Object> discoveryConfig = new HashMap<>();
         discoveryConfig.put(CronTriggerer.CRON_KEY, cronString);

         final BatchSourceConfig batchSourceConfig =
                 BatchSourceConfig.builder()
                         .discoveryTriggererClassName(CronTriggerer.class.getName())
                         .discoveryTriggererConfig(discoveryConfig)
                         .build();

         final SourceConfig sourceConfig =
                 SourceConfig.builder()
                         .batchSourceConfig(batchSourceConfig)
                         .className(BatchDataGeneratorSource.class.getName())
                         .configs(new HashMap<>())
                         .name("BatchDataGenerator")
                         .parallelism(1)
                         .topicName("persistent://public/default/batchdatagenerator")
                         .build();

         final LocalRunner localRunner =
                 LocalRunner.builder()
                         .brokerServiceUrl("pulsar://localhost:6650")
                         .sourceConfig(sourceConfig)
                         .build();

         localRunner.start(false);
         TimeUnit.MINUTES.sleep(30);
         localRunner.stop();

         System.exit(0);
     }
 }
