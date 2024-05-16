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

import java.util.concurrent.Callable;
import picocli.CommandLine;

public abstract class CmdBase implements Callable<Integer> {
    private final CommandLine commander;

    public CmdBase(String cmdName) {
        commander = new CommandLine(this);
        commander.setCommandName(cmdName);
    }

    public boolean run(String[] args) {
        return commander.execute(args) == 0;
    }

    public void parse(String[] args) {
        commander.parseArgs(args);
    }

    /**
     * Validate the CLI arguments.  Default implementation provides validation for the common arguments.
     * Each subclass should call super.validate() and provide validation code specific to the sub-command.
     * @throws Exception
     */
    public void validate() throws Exception {
    }

    // Picocli entrypoint.
    @Override
    public Integer call() throws Exception {
        validate();
        run();
        return 0;
    }

    public abstract void run() throws Exception;


    protected CommandLine getCommander() {
        return commander;
    }

    protected void addCommand(String name, Object cmd) {
        commander.addSubcommand(name, cmd);
    }

    protected void addCommand(String name, Object cmd, String... aliases) {
        commander.addSubcommand(name, cmd, aliases);
    }

    protected class ParameterException extends CommandLine.ParameterException {
        public ParameterException(String msg) {
            super(commander, msg);
        }

        public ParameterException(String msg, Throwable e) {
            super(commander, msg, e);
        }
    }
}
