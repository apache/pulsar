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
package org.apache.pulsar.functions.worker;

import static org.apache.commons.lang3.StringUtils.isBlank;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.CmdGenerateDocs;

/**
 * A starter to start function worker.
 */
@Slf4j
public class FunctionWorkerStarter {

    private static class WorkerArguments {
        @Parameter(
            names = { "-c", "--conf" },
            description = "Configuration File for Function Worker")
        private String configFile;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        private boolean help = false;

        @Parameter(names = {"-g", "--generate-docs"}, description = "Generate docs")
        private boolean generateDocs = false;
    }

    public static void main(String[] args) throws Exception {
        WorkerArguments workerArguments = new WorkerArguments();
        JCommander commander = new JCommander(workerArguments);
        commander.setProgramName("FunctionWorkerStarter");

        // parse args by commander
        commander.parse(args);

        if (workerArguments.help) {
            commander.usage();
            System.exit(1);
            return;
        }

        if (workerArguments.generateDocs) {
            CmdGenerateDocs cmd = new CmdGenerateDocs("pulsar");
            cmd.addCommand("functions-worker", workerArguments);
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
            Runtime.getRuntime().halt(1);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping function worker service...");
            worker.stop();
        }));
    }
}
