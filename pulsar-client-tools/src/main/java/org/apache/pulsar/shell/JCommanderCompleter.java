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
package org.apache.pulsar.shell;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.WrappedParameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pulsar.admin.cli.CmdBase;
import org.jline.builtins.Completers;
import org.jline.reader.Completer;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

/**
 * Convert JCommander instance to JLine3 completers.
 */
public class JCommanderCompleter {

    private JCommanderCompleter() {
    }

    public static List<Completer> createCompletersForCommand(String program,
                                                             JCommander command) {
        command.setProgramName(program);
        return createCompletersForCommand(Collections.emptyList(),
                command,
                List.of(NullCompleter.INSTANCE));
    }

    private static List<Completer> createCompletersForCommand(List<Completer> preCompleters,
                                                              JCommander command,
                                                              List<Completer> postCompleters) {
        List<Completer> all = new ArrayList<>();
        addCompletersForCommand(preCompleters, postCompleters, all, command);
        return all;
    }

    private static void addCompletersForCommand(List<Completer> preCompleters,
                                                List<Completer> postCompleters,
                                                List<Completer> result,
                                                JCommander command) {
        final Collection<Completers.OptDesc> options;
        final Map<String, JCommander> subCommands;

        if (command.getObjects().get(0) instanceof CmdBase) {
            CmdBase cmdBase = (CmdBase) command.getObjects().get(0);
            subCommands = cmdBase.getJcommander().getCommands();
            options = cmdBase.getJcommander().getParameters().stream().map(JCommanderCompleter::createOptionDescriptors)
                    .collect(Collectors.toList());
        } else {
            subCommands = command.getCommands();
            options = command.getParameters().stream().map(JCommanderCompleter::createOptionDescriptors)
                    .collect(Collectors.toList());
        }

        final StringsCompleter cmdStringsCompleter = new StringsCompleter(command.getProgramName());

        for (int i = 0; i < options.size() + 1; i++) {
            List<Completer> completersChain = new ArrayList<>();
            completersChain.addAll(preCompleters);
            completersChain.add(cmdStringsCompleter);
            for (int j = 0; j < i; j++) {
                completersChain.add(new Completers.OptionCompleter(options, preCompleters.size() + 1 + j));
            }
            for (Map.Entry<String, JCommander> subCommand : subCommands.entrySet()) {
                addCompletersForCommand(completersChain, postCompleters, result, subCommand.getValue());
            }
            completersChain.addAll(postCompleters);
            result.add(new OptionStrictArgumentCompleter(completersChain));
        }
    }


    private static Completers.OptDesc createOptionDescriptors(ParameterDescription param) {
        Completer valueCompleter = null;
        boolean isBooleanArg = param.getObject() instanceof Boolean || param.getDefault() instanceof Boolean
                || param.getObject().getClass().isAssignableFrom(Boolean.class);
        if (!isBooleanArg) {
            valueCompleter = Completers.AnyCompleter.INSTANCE;
        }

        final WrappedParameter parameter = param.getParameter();
        String shortOption = null;
        String longOption = null;
        final String[] parameterNames = parameter.names();
        for (String parameterName : parameterNames) {
            if (parameterName.startsWith("--")) {
                longOption = parameterName;
            } else if (parameterName.startsWith("-")) {
                shortOption = parameterName;
            }
        }
        return new Completers.OptDesc(shortOption, longOption, param.getDescription(), valueCompleter);
    }

}
