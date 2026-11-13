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

import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.SystemUtils;
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

    /**
     * Append the environment variables read by the bin/pulsar-admin and bin/pulsar-client scripts to the usage
     * message of the root command. It must be called after all subcommands are added, otherwise they inherit it.
     *
     * @param commander Root commander.
     */
    public static void addEnvironmentVariablesFooter(CommandLine commander) {
        addEnvironmentVariablesFooter(commander, SystemUtils.IS_OS_WINDOWS);
    }

    /**
     * The bin\*.cmd scripts on Windows take the classpath from PULSAR_CLASSPATH, do not read PULSAR_MEM and
     * PULSAR_GC, and do not source conf/pulsar_tools_env.sh.
     */
    @VisibleForTesting
    static void addEnvironmentVariablesFooter(CommandLine commander, boolean windows) {
        List<String> footer = new ArrayList<>();
        footer.add("   JAVA_HOME                Java installation to use (default: java on the PATH)");
        footer.add("   PULSAR_CLIENT_CONF       Configuration file (default: conf/client.conf)");
        footer.add("   PULSAR_LOG_CONF          Log4j configuration file (default: conf/log4j2.yaml)");
        footer.add("   PULSAR_EXTRA_OPTS        Extra options to be passed to the JVM");
        if (windows) {
            footer.add("   PULSAR_CLASSPATH         Extra paths to add to the classpath");
        } else {
            footer.add("   PULSAR_EXTRA_CLASSPATH   Extra paths to add to the classpath");
            footer.add("   PULSAR_MEM               JVM memory options");
            footer.add("   PULSAR_GC                JVM garbage collection options");
        }
        footer.add("   PULSAR_LOG_DIR           Log directory (default: logs)");
        footer.add("   PULSAR_LOG_LEVEL         Log level (default: info)");
        footer.add(windows
                ? "   PULSAR_LOG_ROOT_LEVEL    Root log level (default: %%PULSAR_LOG_LEVEL%%)"
                : "   PULSAR_LOG_ROOT_LEVEL    Root log level (default: $PULSAR_LOG_LEVEL)");
        footer.add("   PULSAR_LOG_FORMAT        Log format, text or json (default: text)");
        if (!windows) {
            footer.add("");
            footer.add("These variables can also be set in conf/pulsar_tools_env.sh");
        }
        commander.getCommandSpec().usageMessage()
                .footerHeading("%nEnvironment variables:%n")
                .footer(footer.toArray(new String[0]));
    }
}
