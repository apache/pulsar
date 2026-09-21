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
package org.apache.pulsar.internal;

import java.io.PrintWriter;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConnectException;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.IDefaultValueProvider;
import picocli.CommandLine.IExecutionStrategy;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

public class CommanderFactory {
    static class CommandExecutionStrategy implements IExecutionStrategy {
        private int preRun(ParseResult parseResult) {
            Object userObject = parseResult.commandSpec().userObject();
            if (userObject instanceof CommandHook) {
                int exitCode = ((CommandHook) userObject).preRun();
                if (exitCode != 0) {
                    return exitCode;
                }
            }

            if (parseResult.hasSubcommand()) {
                return preRun(parseResult.subcommand());
            }

            return 0;
        }


        @Override
        public int execute(ParseResult parseResult) throws ExecutionException, ParameterException {
            int preRunCode = preRun(parseResult);
            if (preRunCode != 0) {
                return preRunCode;
            }

            return new CommandLine.RunLast().execute(parseResult);
        }
    }

    /**
     * createRootCommanderWithHook is used for the root command, which supports the hook feature.
     *
     * @param object               Command class or object.
     * @param defaultValueProvider Default value provider of command.
     * @return Picocli commander.
     */
    public static CommandLine createRootCommanderWithHook(Object object, IDefaultValueProvider defaultValueProvider) {
        CommandLine commander = new CommandLine(object);
        commander.setExecutionStrategy(new CommandExecutionStrategy());
        commander.setExecutionExceptionHandler((ex, commandLine, parseResult) -> {
            PrintWriter errPrinter = commandLine.getErr();
            if (ex instanceof ConnectException) {
                errPrinter.println(ex.getMessage());
                errPrinter.println();
                errPrinter.println("Error connecting to Pulsar");
                return 1;
            } else if (ex instanceof PulsarAdminException) {
                errPrinter.println(((PulsarAdminException) ex).getHttpError());
                errPrinter.println();
                errPrinter.println("Reason: " + ex.getMessage());
                return 1;
            }
            throw ex;
        });
        if (defaultValueProvider != null) {
            commander.setDefaultValueProvider(defaultValueProvider);
        }
        return commander;
    }
}
