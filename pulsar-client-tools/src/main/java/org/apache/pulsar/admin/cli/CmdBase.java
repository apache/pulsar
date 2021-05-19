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
package org.apache.pulsar.admin.cli;

import com.beust.jcommander.DefaultUsageFormatter;
import com.beust.jcommander.IUsageFormatter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConnectException;

import java.util.function.Supplier;

public abstract class CmdBase {
    protected final JCommander jcommander;
    private final Supplier<PulsarAdmin> adminSupplier;
    private PulsarAdmin admin;
    private IUsageFormatter usageFormatter;

    @Parameter(names = { "-h", "--help" }, help = true, hidden = true)
    private boolean help;

    public CmdBase(String cmdName, Supplier<PulsarAdmin> adminSupplier) {
        this.adminSupplier = adminSupplier;
        jcommander = new JCommander();
        usageFormatter = new CmdUsageFormatter(jcommander);
        jcommander.setProgramName("pulsar-admin " + cmdName);
        jcommander.setUsageFormatter(usageFormatter);
    }

    protected IUsageFormatter getUsageFormatter() {
        if (usageFormatter == null) {
             usageFormatter = new DefaultUsageFormatter(jcommander);
        }
        return usageFormatter;
    }

    private void tryShowCommandUsage() {
        try {
            String chosenCommand = jcommander.getParsedCommand();
            getUsageFormatter().usage(chosenCommand);
        } catch (Exception e) {
            // it is caused by an invalid command, the invalid command can not be parsed
            System.err.println("Invalid command, please use `pulsar-admin --help` to check out how to use");
        }
    }

    public boolean run(String[] args) {
        try {
            jcommander.parse(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            tryShowCommandUsage();
            return false;
        }

        String cmd = jcommander.getParsedCommand();
        if (cmd == null) {
            jcommander.usage();
            return false;
        } else {
            JCommander obj = jcommander.getCommands().get(cmd);
            CliCommand cmdObj = (CliCommand) obj.getObjects().get(0);

            try {
                cmdObj.run();
                return true;
            } catch (ParameterException e) {
                System.err.println(e.getMessage());
                System.err.println();
                return false;
            } catch (ConnectException e) {
                System.err.println(e.getMessage());
                System.err.println();
                System.err.println("Error connecting to: " + getAdmin().getServiceUrl());
                return false;
            } catch (PulsarAdminException e) {
                System.err.println(e.getHttpError());
                System.err.println();
                System.err.println("Reason: " + e.getMessage());
                return false;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    protected PulsarAdmin getAdmin() {
        if (admin == null) {
            admin = adminSupplier.get();
        }
        return admin;
    }
}
