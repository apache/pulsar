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
package org.apache.pulsar.broker.tools;

import org.apache.bookkeeper.tools.framework.Cli;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;

/**
 * <b>broker-tool</b> is used for operations on a specific broker.
 */
public class BrokerTool {

    public static final String NAME = "broker-tool";

    public static int run(String[] args) {
        CliSpec.Builder<CliFlags> specBuilder = CliSpec.newBuilder()
            .withName(NAME)
            .withUsage(NAME + " [flags] [commands]")
            .withDescription(NAME + " is used for operations on a specific broker")
            .withFlags(new CliFlags())
            .withConsole(System.out)
            .addCommand(new LoadReportCommand())
            .addCommand(new GenerateDocsCommand());

        CliSpec<CliFlags> spec = specBuilder.build();

        return Cli.runCli(spec, args);
    }

    public static void main(String[] args) {
        int retCode = run(args);
        Runtime.getRuntime().exit(retCode);
    }
}
