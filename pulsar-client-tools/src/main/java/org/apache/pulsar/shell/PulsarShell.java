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
import com.beust.jcommander.Parameter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;

/**
 * Main Pulsar shell class invokable from the pulsar-shell script.
 */
public class PulsarShell {

    private static final String EXIT_MESSAGE = "Goodbye!";
    private static final String PROPERTY_PERSIST_HISTORY_ENABLED = "shellHistoryPersistEnabled";
    private static final String PROPERTY_PERSIST_HISTORY_PATH = "shellHistoryPersistPath";
    private static final String CHECKMARK = new String(Character.toChars(0x2714));
    private static final String XMARK = new String(Character.toChars(0x2716));
    private static final AttributedStyle LOG_STYLE = AttributedStyle.DEFAULT
            .foreground(25, 143, 255)
            .background(230, 241, 255);

    static final class ShellOptions {

        @Parameter(names = {"-h", "--help"}, help = true, description = "Show this help.")
        boolean help;
    }

    static final class MainOptions {

        @Parameter(names = {"-f", "--filename"}, description = "Input filename with a list of commands to be executed."
                + " Each command must be separated by a newline.")
        String filename;

        @Parameter(names = {"-e", "--exit-on-error"}, description = "If true, the shell will be interrupted "
                + "if a command throws an exception.")
        boolean exitOnError;

        @Parameter(names = {"-"}, description = "Read commands from the standard input.")
        boolean readFromStdin;

        @Parameter(names = {"-np", "--no-progress"}, description = "Display raw output of the commands without the "
                + "fancy progress visualization.")
        boolean noProgress;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: pulsar-shell CONF_FILE_PATH");
            System.exit(0);
            return;
        }

        String configFile = args[0];
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            properties.load(fis);
        }
        new PulsarShell().run(Arrays.copyOfRange(args, 1, args.length), properties);
    }

    public void run(String[] args, Properties properties) throws Exception {
        final Terminal terminal = TerminalBuilder.builder().build();
        run(args, properties, (providersMap) -> {
            List<Completer> completers = new ArrayList<>();
            String serviceUrl = "";
            String adminUrl = "";
            for (ShellCommandsProvider provider : providersMap.values()) {
                provider.setupState(properties);
                final JCommander jCommander = provider.getJCommander();
                if (jCommander != null) {
                    jCommander.createDescriptions();
                    completers.addAll(JCommanderCompleter
                            .createCompletersForCommand(provider.getName(), jCommander));
                }

                final String providerServiceUrl = provider.getServiceUrl();
                if (providerServiceUrl != null) {
                    serviceUrl = providerServiceUrl;
                }
                final String providerAdminUrl = provider.getAdminUrl();
                if (providerAdminUrl != null) {
                    adminUrl = providerAdminUrl;
                }
            }

            Completer completer = new AggregateCompleter(completers);

            LineReaderBuilder readerBuilder = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .completer(completer)
                    .variable(LineReader.INDENTATION, 2)
                    .option(LineReader.Option.INSERT_BRACKET, true);

            configureHistory(properties, readerBuilder);
            LineReader reader = readerBuilder.build();

            final String welcomeMessage =
                    String.format("Welcome to Pulsar shell!\n  Service URL: %s\n  Admin URL: %s\n\n "
                                    + "Type 'help' to get started or try the autocompletion (TAB button).\n",
                            serviceUrl, adminUrl);
            output(welcomeMessage, terminal);
            return reader;
        }, (providerMap) -> terminal);
    }

    private void configureHistory(Properties properties, LineReaderBuilder readerBuilder) {
        final boolean isPersistHistoryEnabled = Boolean.parseBoolean(properties.getProperty(
                PROPERTY_PERSIST_HISTORY_ENABLED, "true"));
        if (isPersistHistoryEnabled) {
            final String persistHistoryPath = properties
                    .getProperty(PROPERTY_PERSIST_HISTORY_PATH, Paths.get(System.getProperty("user.home"),
                            ".pulsar-shell.history").toFile().getAbsolutePath());
            readerBuilder
                    .variable(LineReader.HISTORY_FILE, persistHistoryPath);
        }
    }

    private interface CommandReader {
        String readCommand() throws InterruptShellException;
    }

    private static class InterruptShellException extends RuntimeException {
    }

    private static class CommandsInfo {

        @AllArgsConstructor
        static class ExecutedCommandInfo {
            String command;
            boolean ok;
        }
        int totalCommands;
        List<ExecutedCommandInfo> executedCommands = new ArrayList<>();
        String executingCommand;
    }

    public void run(String[] args,
                    Properties properties,
                    Function<Map<String, ShellCommandsProvider>, LineReader> readerBuilder,
                    Function<Map<String, ShellCommandsProvider>, Terminal> terminalBuilder) throws Exception {
        System.setProperty("org.jline.terminal.dumb", "true");

        /**
         * Options read from System.args
         */
        final JCommander mainCommander = new JCommander();
        final MainOptions mainOptions = new MainOptions();
        mainCommander.addObject(mainOptions);
        try {
            mainCommander.parse(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            mainCommander.usage();
            exit(1);
            return;
        }
        /**
         * Options read from the shell session
         */
        final JCommander shellCommander = new JCommander();
        final ShellOptions shellOptions = new ShellOptions();
        shellCommander.addObject(shellOptions);

        final Map<String, ShellCommandsProvider> providersMap = registerProviders(shellCommander, properties);

        final LineReader reader = readerBuilder.apply(providersMap);
        final Terminal terminal = terminalBuilder.apply(providersMap);

        CommandReader commandReader;
        CommandsInfo commandsInfo = null;

        if (mainOptions.readFromStdin && mainOptions.filename != null) {
            throw new IllegalArgumentException("Cannot use stdin and -f/--filename option at same time");
        }
        boolean isNonInteractiveMode = mainOptions.filename != null || mainOptions.readFromStdin;

        if (isNonInteractiveMode) {
            final List<String> lines;
            if (mainOptions.filename != null) {
                lines = Files.readAllLines(Paths.get(mainOptions.filename))
                        .stream()
                        .filter(s -> !s.isBlank())
                        .collect(Collectors.toList());
            } else {
                try (BufferedReader stdinReader = new BufferedReader(new InputStreamReader(System.in))) {
                    lines = stdinReader.lines().filter(s -> !s.isBlank()).collect(Collectors.toList());
                }
            }
            if (!mainOptions.noProgress) {
                commandsInfo = new CommandsInfo();
                commandsInfo.totalCommands = lines.size();
            }

            final CommandsInfo finalCommandsInfo = commandsInfo;
            commandReader = new CommandReader() {
                private int index = 0;

                @Override
                public String readCommand() {
                    if (index == lines.size()) {
                        throw new InterruptShellException();
                    }
                    final String command = lines.get(index++).trim();
                    if (finalCommandsInfo != null) {
                        finalCommandsInfo.executingCommand = command;
                    } else {
                        output(String.format("[%d/%d] Executing %s", new Object[]{index,
                                lines.size(), command}), terminal);
                    }
                    return command;
                }
            };
        } else {
            final String prompt = createPrompt();

            commandReader = () -> {
                try {
                    return reader.readLine(prompt).trim();
                } catch (org.jline.reader.UserInterruptException userInterruptException) {
                    throw new InterruptShellException();
                }
            };

        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> quit(terminal)));
        while (true) {
            String line;
            try {
                line = commandReader.readCommand();
            } catch (InterruptShellException interruptShellException) {
                exit(0);
                return;
            }
            if (line.isBlank()) {
                continue;
            }
            if (isQuitCommand(line)) {
                exit(0);
                return;
            }
            final List<String> words = parseLine(reader, line);

            if (shellOptions.help) {
                shellCommander.usage();
                continue;
            }

            final ShellCommandsProvider pulsarShellCommandsProvider = getProviderFromArgs(shellCommander, words);
            if (pulsarShellCommandsProvider == null) {
                shellCommander.usage();
                continue;
            }
            String[] argv = extractAndConvertArgs(words);
            boolean commandOk = false;
            try {
                printExecutingCommands(terminal, commandsInfo, false);
                commandOk = pulsarShellCommandsProvider.runCommand(argv);
            } catch (Throwable t) {
                t.printStackTrace(terminal.writer());
            } finally {
                if (commandsInfo != null) {
                    commandsInfo.executingCommand = null;
                    commandsInfo.executedCommands.add(new CommandsInfo.ExecutedCommandInfo(line, commandOk));
                    printExecutingCommands(terminal, commandsInfo, true);
                }
                pulsarShellCommandsProvider.cleanupState(properties);

            }
            if (mainOptions.exitOnError && !commandOk) {
                exit(1);
                return;
            }
        }
    }

    private void printExecutingCommands(Terminal terminal, CommandsInfo commandsInfo, boolean printExecuted) {
        if (commandsInfo == null) {
            return;
        }
        terminal.puts(InfoCmp.Capability.clear_screen);
        terminal.flush();
        int index = 1;
        if (printExecuted) {
            for (CommandsInfo.ExecutedCommandInfo executedCommand : commandsInfo.executedCommands) {
                String icon = executedCommand.ok ? CHECKMARK : XMARK;
                final String ansiLog = new AttributedStringBuilder()
                        .style(LOG_STYLE)
                        .append(String.format("[%d/%d] %s %s", new Object[]{index++, commandsInfo.totalCommands,
                                icon, executedCommand.command}))
                        .toAnsi();
                output(ansiLog, terminal);
            }
        } else {
            index = commandsInfo.executedCommands.size() + 1;
        }
        if (commandsInfo.executingCommand != null) {
            final String ansiLog = new AttributedStringBuilder()
                    .style(LOG_STYLE)
                    .append(String.format("[%d/%d] Executing %s", new Object[]{index,
                            commandsInfo.totalCommands, commandsInfo.executingCommand}))
                    .toAnsi();
            output(ansiLog, terminal);
        }
    }

    private static ShellCommandsProvider getProviderFromArgs(JCommander mainCommander, List<String> words) {
        final String providerCmd = words.get(0);
        final JCommander commander = mainCommander.getCommands().get(providerCmd);
        if (commander == null) {
            return null;
        }
        return (ShellCommandsProvider) commander.getObjects().get(0);
    }

    private static String createPrompt() {
        return new AttributedStringBuilder()
                .style(LOG_STYLE)
                .append("pulsar>")
                .style(AttributedStyle.DEFAULT)
                .append(" ")
                .toAnsi();
    }

    private static List<String> parseLine(LineReader reader, String line) {
        final ParsedLine pl = reader.getParser().parse(line, 0);
        final List<String> words = pl.words();
        return words;
    }

    private static void quit(Terminal terminal) {
        output(EXIT_MESSAGE, terminal);
    }

    private static void output(String message, Terminal terminal) {
        terminal.writer().println(message);
        terminal.writer().flush();
    }

    private static boolean isQuitCommand(String line) {
        return line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit");
    }

    private static String[] extractAndConvertArgs(List<String> words) {
        List<String> parsed = new ArrayList<>();
        for (String s : words.subList(1, words.size())) {
            if (s.startsWith("-") && s.contains("=")) {
                final String[] split = s.split("=", 2);
                parsed.add(split[0]);
                parsed.add(split[1]);
            } else {
                parsed.add(s);
            }
        }

        String[] argv = parsed.toArray(new String[parsed.size()]);
        return argv;
    }

    private Map<String, ShellCommandsProvider> registerProviders(JCommander commander, Properties properties)
            throws Exception {
        final Map<String, ShellCommandsProvider> providerMap = new HashMap<>();
        registerProvider(createAdminShell(properties), commander, providerMap);
        registerProvider(createClientShell(properties), commander, providerMap);
        return providerMap;
    }

    protected AdminShell createAdminShell(Properties properties) throws Exception {
        return new AdminShell(properties);
    }

    protected ClientShell createClientShell(Properties properties) {
        return new ClientShell(properties);
    }

    private static void registerProvider(ShellCommandsProvider provider,
                                         JCommander commander,
                                         Map<String, ShellCommandsProvider> providerMap) {

        final String name = provider.getName();
        commander.addCommand(name, provider);
        providerMap.put(name, provider);
    }

    protected void exit(int exitCode) {
        System.exit(exitCode);
    }

}
