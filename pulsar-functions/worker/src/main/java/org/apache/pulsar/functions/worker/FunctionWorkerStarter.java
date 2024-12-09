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
package org.apache.pulsar.functions.worker;

import static org.apache.commons.lang3.StringUtils.isBlank;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ShutdownUtil;
import org.apache.pulsar.docs.tools.CmdGenerateDocs;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;

/**
 * A starter to start function worker.
 */
@Slf4j
public class FunctionWorkerStarter {

    @Command(name = "functions-worker", showDefaultValues = true, scope = ScopeType.INHERIT)
    private static class WorkerArguments {
        @Option(
            names = { "-c", "--conf" },
            description = "Configuration File for Function Worker")
        private String configFile;

        @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message")
        private boolean help = false;

        @Option(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }


    public static void main(String[] args) throws Exception {
        WorkerArguments workerArguments = new WorkerArguments();
        CommandLine commander = new CommandLine(workerArguments);
        commander.setCommandName("FunctionWorkerStarter");

        commander.parseArgs(args);

        if (workerArguments.help) {
            commander.usage(commander.getOut());
            return;
        }

        if (workerArguments.generateDocs) {
            CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
            cmd.addCommand("functions-worker", commander);
            cmd.run(null);
            return;
        }

        WorkerConfig workerConfig;
        if (isBlank(workerArguments.configFile)) {
            workerConfig = new WorkerConfig();
        } else {
            workerConfig = WorkerConfig.load(workerArguments.configFile);
        }

        final Worker worker = new Worker(workerConfig);
        try {
            worker.start();
        } catch (Throwable th) {
            log.error("Encountered error in function worker.", th);
            worker.stop();
            ShutdownUtil.triggerImmediateForcefulShutdown();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping function worker service...");
            worker.stop();
        }));
    }
}
