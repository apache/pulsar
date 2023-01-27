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

import static java.lang.annotation.ElementType.FIELD;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.WrappedParameter;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.admin.cli.CmdBase;
import org.apache.pulsar.shell.config.ConfigStore;
import org.jline.builtins.Completers;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

/**
 * Convert JCommander instance to JLine3 completers.
 */
public class JCommanderCompleter {

    @AllArgsConstructor
    @Getter
    public static class ShellContext {
        private final ConfigStore configStore;
    }

    private JCommanderCompleter() {
    }

    public static List<Completer> createCompletersForCommand(String program,
                                                             JCommander command,
                                                             ShellContext shellContext) {
        command.setProgramName(program);
        return createCompletersForCommand(Collections.emptyList(),
                command,
                Arrays.asList(NullCompleter.INSTANCE),
                shellContext);
    }

    private static List<Completer> createCompletersForCommand(List<Completer> preCompleters,
                                                              JCommander command,
                                                              List<Completer> postCompleters,
                                                              ShellContext shellContext) {
        List<Completer> all = new ArrayList<>();
        addCompletersForCommand(preCompleters, postCompleters, all, command, shellContext);
        return all;
    }

    private static void addCompletersForCommand(List<Completer> preCompleters,
                                                List<Completer> postCompleters,
                                                List<Completer> result,
                                                JCommander command,
                                                ShellContext shellContext) {
        final Collection<Completers.OptDesc> options;
        final Map<String, JCommander> subCommands;
        final ParameterDescription mainParameterValue;

        if (command.getObjects().get(0) instanceof CmdBase) {
            CmdBase cmdBase = (CmdBase) command.getObjects().get(0);
            subCommands = cmdBase.getJcommander().getCommands();
            mainParameterValue = cmdBase.getJcommander().getMainParameter() == null ? null :
                    cmdBase.getJcommander().getMainParameterValue();
            options = cmdBase.getJcommander().getParameters()
                    .stream()
                    .map(option -> createOptionDescriptors(option, shellContext))
                    .collect(Collectors.toList());
        } else {
            subCommands = command.getCommands();
            mainParameterValue = command.getMainParameter() == null ? null : command.getMainParameterValue();
            options = command.getParameters()
                    .stream()
                    .map(option -> createOptionDescriptors(option, shellContext))
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
                addCompletersForCommand(completersChain, postCompleters, result, subCommand.getValue(), shellContext);
            }
            if (mainParameterValue != null) {
                final Completer customCompleter = getCustomCompleter(mainParameterValue, shellContext);
                if (customCompleter != null) {
                    completersChain.add(customCompleter);
                }
            }
            completersChain.addAll(postCompleters);
            result.add(new OptionStrictArgumentCompleter(completersChain));
        }
    }


    @SneakyThrows
    private static Completers.OptDesc createOptionDescriptors(ParameterDescription param, ShellContext shellContext) {
        Completer valueCompleter = getCompleter(param, shellContext);
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

    @SneakyThrows
    private static Completer getCompleter(ParameterDescription param, ShellContext shellContext) {

        Completer valueCompleter = null;
        boolean isBooleanArg = param.getObject() instanceof Boolean || param.getDefault() instanceof Boolean
                || param.getObject().getClass().isAssignableFrom(Boolean.class);
        if (!isBooleanArg) {
            valueCompleter = getCustomCompleter(param, shellContext);
            if (valueCompleter == null) {
                valueCompleter = Completers.AnyCompleter.INSTANCE;
            }
        }
        return valueCompleter;
    }

    @SneakyThrows
    private static Completer getCustomCompleter(ParameterDescription param, ShellContext shellContext) {
        Completer valueCompleter = null;
        final Field reflField = param.getParameterized().getClass().getDeclaredField("field");
        reflField.setAccessible(true);
        final Field field = (Field) reflField.get(param.getParameterized());
        final ParameterCompleter parameterCompleter = field.getAnnotation(ParameterCompleter.class);
        if (parameterCompleter != null) {
            final ParameterCompleter.Type completer = parameterCompleter.type();
            if (completer == ParameterCompleter.Type.FILES) {
                valueCompleter = new Completers.FilesCompleter(ConfigShell.resolveLocalFile("."));
            } else if (completer == ParameterCompleter.Type.CONFIGS) {
                valueCompleter = new Completer() {
                    @Override
                    @SneakyThrows
                    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
                        new StringsCompleter(shellContext.configStore.listConfigs()
                                .stream().map(ConfigStore.ConfigEntry::getName).collect(Collectors.toList()))
                                .complete(reader, line, candidates);
                    }
                };
            }
        }
        return valueCompleter;
    }

    @Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
    @Target({ FIELD })
    public @interface ParameterCompleter {

        enum Type {
            FILES,
            CONFIGS;
        }

        Type type();

    }

}
