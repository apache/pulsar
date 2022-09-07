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
package org.apache.pulsar.admin.cli.examples;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.admin.cli.extensions.CommandExecutionContext;
import org.apache.pulsar.admin.cli.extensions.CustomCommand;
import org.apache.pulsar.admin.cli.extensions.CustomCommandFactory;
import org.apache.pulsar.admin.cli.extensions.CustomCommandGroup;
import org.apache.pulsar.admin.cli.extensions.ParameterDescriptor;
import org.apache.pulsar.admin.cli.extensions.ParameterType;
import org.apache.pulsar.common.policies.data.TopicStats;

@Slf4j
public class MyCommandFactory implements CustomCommandFactory {
  @Override
  public List<CustomCommandGroup> commandGroups(CommandExecutionContext context) {
    return Arrays.asList(
            new MyCustomCommandGroup());
  }

    private static class MyCustomCommandGroup implements CustomCommandGroup {
        @Override
        public String name() {
          return "customgroup";
        }

        @Override
        public String description() {
          return "Custom group 1 description";
        }

        @Override
        public List<CustomCommand> commands(CommandExecutionContext context) {
          return Arrays.asList(new Command1(), new Command2());
        }

        private static class Command1 implements CustomCommand {
            @Override
            public String name() {
              return "command1";
            }

            @Override
            public String description() {
              return "Command 1 description";
            }

            @Override
            public List<ParameterDescriptor> parameters() {
              return Arrays.asList(
                  ParameterDescriptor.builder()
                      .description("Operation type")
                      .type(ParameterType.STRING)
                      .names(Arrays.asList("--type", "-t"))
                      .required(true)
                      .build(),
                  ParameterDescriptor.builder()
                      .description("Topic")
                      .type(ParameterType.STRING)
                      .mainParameter(true)
                      .names(Arrays.asList("topic"))
                      .required(true)
                      .build());
            }

            @Override
            public boolean execute(
                    Map<String, Object> parameters, CommandExecutionContext context)
                throws Exception {
              System.out.println(
                  "Execute: " + parameters + " properties " + context.getConfiguration());
              String destination = parameters.getOrDefault("topic", "").toString();
              TopicStats stats = context.getPulsarAdmin().topics().getStats(destination);
              System.out.println("Topic stats: " + stats);
              return false;
            }
        }

        private static class Command2 implements CustomCommand {
            @Override
            public String name() {
                return "command2";
            }

            @Override
            public String description() {
                return "Command 2 description";
            }

            @Override
            public List<ParameterDescriptor> parameters() {
                return Arrays.asList(
                        ParameterDescriptor.builder()
                                .description("mystring")
                                .type(ParameterType.STRING)
                                .names(Arrays.asList("-s"))
                                .build(),
                        ParameterDescriptor.builder()
                                .description("myint")
                                .type(ParameterType.INTEGER)
                                .names(Arrays.asList("-i"))
                                .build(),
                        ParameterDescriptor.builder()
                                .description("myboolean")
                                .type(ParameterType.BOOLEAN)
                                .names(Arrays.asList("-b"))
                                .build(),
                        ParameterDescriptor.builder()
                                .description("mybooleanflag")
                                .type(ParameterType.BOOLEAN_FLAG)
                                .names(Arrays.asList("-bf"))
                                .build(),
                        ParameterDescriptor.builder()
                                .description("main")
                                .type(ParameterType.STRING)
                                .mainParameter(true)
                                .names(Arrays.asList("main"))
                                .build());
            }

            @Override
            public boolean execute(
                    Map<String, Object> parameters, CommandExecutionContext context)
                    throws Exception {
                System.out.println(
                        "Execute: " + parameters + " properties " + context.getConfiguration());
                return false;
            }
        }
  }
}
