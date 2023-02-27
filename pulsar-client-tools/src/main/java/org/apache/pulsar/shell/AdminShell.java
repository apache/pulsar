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
package org.apache.pulsar.shell;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import java.util.Properties;
import org.apache.pulsar.admin.cli.PulsarAdminTool;

/**
 * Pulsar Admin tool extension for Pulsar shell.
 */
@Parameters(commandDescription = "Admin console")
public class AdminShell extends PulsarAdminTool implements ShellCommandsProvider {

    public AdminShell(Properties properties) throws Exception {
        super(properties);
    }

    @Override
    public String getName() {
        return "admin";
    }

    @Override
    public String getServiceUrl() {
        return null;
    }

    @Override
    public String getAdminUrl() {
        return rootParams.getServiceUrl();
    }

    @Override
    public void setupState(Properties properties) {
        getJCommander().setProgramName(getName());
        setupCommands();
    }

    @Override
    public JCommander getJCommander() {
        return jcommander;
    }

    @Override
    public void cleanupState(Properties properties) {
        rootParams = new RootParams();
        initRootParamsFromProperties(properties);
        initJCommander();
    }


    @Override
    public boolean runCommand(String[] args) throws Exception {
        return run(args);
    }
}
