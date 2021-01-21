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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.DefaultUsageFormatter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;

public class CmdUsageFormatter extends DefaultUsageFormatter {

    /**
     * The commands in this set are hidden and not shown to users
     */
    private Set<String> deprecatedCommands = new HashSet<>();

    private final JCommander commander;

    public CmdUsageFormatter(JCommander commander) {
        super(commander);
        this.commander = commander;
    }

    /**
     * This method is copied from DefaultUsageFormatter,
     * but the ability to skip deprecated commands is added.
     * @param out
     * @param indentCount
     * @param descriptionIndent
     * @param indent
     */
    @Override
    public void appendCommands(StringBuilder out, int indentCount, int descriptionIndent, String indent) {
        out.append(indent + "  Commands:\n");

        for (Map.Entry<JCommander.ProgramName, JCommander> commands : commander.getRawCommands().entrySet()) {
            Object arg = commands.getValue().getObjects().get(0);
            Parameters p = arg.getClass().getAnnotation(Parameters.class);

            if (p == null || !p.hidden()) {
                JCommander.ProgramName progName = commands.getKey();
                String dispName = progName.getDisplayName();
                //skip the deprecated command
                if(deprecatedCommands.contains(dispName)){
                    continue;
                }
                String description = indent + s(4) + dispName + s(6) + getCommandDescription(progName.getName());
                wrapDescription(out, indentCount + descriptionIndent, description);
                out.append("\n");

                JCommander jc = commander.findCommandByAlias(progName.getName());
                jc.getUsageFormatter().usage(out, indent + s(6));
                out.append("\n");
            }
        }
    }

    public void addDeprecatedCommand(String command) {
        this.deprecatedCommands.add(command);
    }

    public void removeDeprecatedCommand(String command) {
        this.deprecatedCommands.remove(command);
    }

    public void clearDeprecatedCommand(){
        this.deprecatedCommands.clear();
    }

}
