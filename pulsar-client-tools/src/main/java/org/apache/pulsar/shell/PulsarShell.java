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
import com.beust.jcommander.Parameter;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.pulsar.shell.config.ConfigStore;
import org.apache.pulsar.shell.config.FileConfigStore;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.DefaultParser;
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
    private static final String PROPERTY_PULSAR_SHELL_DIR = "shellHistoryDirectory";
    private static final String PROPERTY_PERSIST_HISTORY_ENABLED = "shellHistoryPersistEnabled";
    private static final String CHECKMARK = new String(Character.toChars(0x2714));
    private static final String XMARK = new String(Character.toChars(0x2716));
    private static final AttributedStyle LOG_STYLE = AttributedStyle.DEFAULT
            .foreground(25, 143, 255)
            .background(230, 241, 255);

    private static final Substitutor[] SUBSTITUTORS = {
            (str, vars) -> new StringSubstitutor(vars, "${", "}", '\\').replace(str),
            (str, vars) -> {
                // unfortunately StringSubstitutor doesn't handle empty suffix regex
                if (str.startsWith("\\$")) {
                    return str.substring(1);
                }
                if (str.startsWith("$")) {
                    final String key = str.substring(1);
                    if (!vars.containsKey(key)) {
                        return str;
                    }
                    return vars.get(key);
                }
                return str;
            }
    };

    private static final String DEFAULT_PULSAR_SHELL_ROOT_DIRECTORY = computeDefaultPulsarShellRootDirectory();

    interface Substitutor {
        String replace(String str, Map<String, String> vars);
    }

    static final class ShellOptions {

        @Parameter(names = {"-h", "--help"}, help = true, description = "Show this help.")
        boolean help;
    }

    static final class MainOptions {

        @Parameter(names = {"-c", "--config"}, description = "Client configuration file.")
        String configFile;

        @Parameter(names = {"-f", "--filename"}, description = "Input filename with a list of commands to be executed."
                + " Each command must be separated by a newline.")
        String filename;

        @Parameter(names = {"--fail-on-error"}, description = "If true, the shell will be interrupted "
                + "if a command throws an exception.")
        boolean failOnError;

        @Parameter(names = {"-"}, description = "Read commands from the standard input.")
        boolean readFromStdin;

        @Parameter(names = {"-e", "--execute-command"}, description = "Execute this command and exit.")
        String inlineCommand;

        @Parameter(names = {"-np", "--no-progress"}, description = "Display raw output of the commands without the "
                + "fancy progress visualization.")
        boolean noProgress;
    }

    enum ExecState {
        IDLE,
        RUNNING
    }
    private Properties properties;
    @Getter
    private final ConfigStore configStore;
    private final File pulsarShellDir;
    private final JCommander mainCommander;
    private final MainOptions mainOptions;
    private JCommander shellCommander;
    private Function<Map<String, ShellCommandsProvider>, InteractiveLineReader> readerBuilder;
    private InteractiveLineReader reader;
    private final ConfigShell configShell;
    private ExecState execState = ExecState.IDLE;

    public PulsarShell(String args[]) throws IOException {
        this(args, new Properties());
    }
    public PulsarShell(String args[], Properties props) throws IOException {
        properties = props;
        mainCommander = new JCommander();
        mainOptions = new MainOptions();
        mainCommander.addObject(mainOptions);
        try {
            mainCommander.parse(args);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.err.println();
            mainCommander.usage();
            exit(1);
            throw new IllegalArgumentException(e);
        }

        pulsarShellDir = computePulsarShellFile();
        Files.createDirectories(pulsarShellDir.toPath());
        System.out.println(String.format("Using directory: %s", pulsarShellDir.getAbsolutePath()));

        ConfigStore.ConfigEntry defaultConfig = null;
        String configFile;

        if (mainOptions.configFile != null) {
            configFile = mainOptions.configFile;
        } else {
            configFile = System.getProperty("pulsar.shell.config.default");
        }
        if (configFile != null) {
            final String defaultConfigValue =
                    new String(Files.readAllBytes(new File(configFile).toPath()), StandardCharsets.UTF_8);
            defaultConfig = new ConfigStore.ConfigEntry(ConfigStore.DEFAULT_CONFIG, defaultConfigValue);
        }

        configStore = new FileConfigStore(
                Paths.get(pulsarShellDir.getAbsolutePath(), "configs.json").toFile(),
                defaultConfig);

        final ConfigStore.ConfigEntry lastUsed = configStore.getLastUsed();
        String configName = ConfigStore.DEFAULT_CONFIG;
        if (lastUsed != null) {
            properties.load(new StringReader(lastUsed.getValue()));
            configName = lastUsed.getName();
        } else if (defaultConfig != null) {
            properties.load(new StringReader(defaultConfig.getValue()));
        }
        configShell = new ConfigShell(this, configName);
    }

    private static File computePulsarShellFile() {
        String dir = System.getProperty(PROPERTY_PULSAR_SHELL_DIR, null);
        if (dir == null) {
            return Paths.get(DEFAULT_PULSAR_SHELL_ROOT_DIRECTORY, ".pulsar-shell").toFile();
        } else {
            return new File(dir);
        }
    }

    /**
     * Compute the default Pulsar shell root directory.
     * If system property "user.home" returns invalid value, the default value will be the current directory.
     * @return
     */
    private static String computeDefaultPulsarShellRootDirectory() {
        final String userHome = System.getProperty("user.home");
        if (!StringUtils.isBlank(userHome) && !"?".equals(userHome)) {
            return userHome;
        }
        return System.getProperty("user.dir");
    }

    public static void main(String[] args) throws Exception {
        new PulsarShell(args).run();
    }

    public void reload(Properties properties) throws Exception {
        this.properties = properties;
        final Map<String, ShellCommandsProvider> providersMap = registerProviders(shellCommander, properties);
        reader = readerBuilder.apply(providersMap);
    }

    public void run() throws Exception {
        final Terminal terminal = TerminalBuilder.builder()
                .nativeSignals(true)
                .signalHandler(signal -> {
                    if (signal == Terminal.Signal.INT || signal == Terminal.Signal.QUIT) {
                        if (execState == ExecState.RUNNING) {
                            throw new InterruptShellException();
                        } else {
                            exit(0);
                        }
                    }
                })
                .build();
        run((providersMap) -> {
            List<Completer> completers = new ArrayList<>();
            String serviceUrl = "";
            String adminUrl = "";
            final JCommanderCompleter.ShellContext shellContext = new JCommanderCompleter.ShellContext(configStore);
            for (ShellCommandsProvider provider : providersMap.values()) {
                provider.setupState(properties);
                final JCommander jCommander = provider.getJCommander();
                if (jCommander != null) {
                    jCommander.createDescriptions();
                    completers.addAll(JCommanderCompleter
                            .createCompletersForCommand(provider.getName(), jCommander, shellContext));
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
                    .parser(new DefaultParser().eofOnUnclosedQuote(true))
                    .completer(completer)
                    .variable(LineReader.INDENTATION, 2)
                    .option(LineReader.Option.INSERT_BRACKET, true);

            configureHistory(properties, readerBuilder);
            LineReader reader = readerBuilder.build();

            final String welcomeMessage =
                    String.format("Welcome to Pulsar shell!\n  %s: %s\n  %s: %s\n\n"
                                    + "Type %s to get started or try the autocompletion (TAB button).\n"
                                    + "Type %s or %s to end the shell session.\n",
                            new AttributedStringBuilder().style(AttributedStyle.BOLD).append("Service URL").toAnsi(),
                            serviceUrl,
                            new AttributedStringBuilder().style(AttributedStyle.BOLD).append("Admin URL").toAnsi(),
                            adminUrl,
                            new AttributedStringBuilder().style(AttributedStyle.BOLD).append("help").toAnsi(),
                            new AttributedStringBuilder().style(AttributedStyle.BOLD).append("exit").toAnsi(),
                            new AttributedStringBuilder().style(AttributedStyle.BOLD).append("quit").toAnsi());
            output(welcomeMessage, terminal);
            String promptMessage;
            if (configShell.getCurrentConfig() != null) {
                promptMessage = String.format("%s(%s)",
                        configShell.getCurrentConfig(), getHostFromUrl(serviceUrl));
            } else {
                promptMessage = getHostFromUrl(serviceUrl);
            }
            final String prompt = createPrompt(promptMessage);
            return new InteractiveLineReader() {
                @Override
                public String readLine() {
                    return reader.readLine(prompt);
                }

                @Override
                public List<String> parseLine(String line) {
                    return reader.getParser().parse(line, 0).words();
                }
            };
        }, (providerMap) -> terminal);
    }

    private void configureHistory(Properties properties, LineReaderBuilder readerBuilder) {
        final boolean isPersistHistoryEnabled = Boolean.parseBoolean(properties.getProperty(
                PROPERTY_PERSIST_HISTORY_ENABLED, "true"));
        if (isPersistHistoryEnabled) {
            final String historyPath = Paths.get(pulsarShellDir.getAbsolutePath(), "history")
                    .toFile().getAbsolutePath();
            readerBuilder.variable(LineReader.HISTORY_FILE, historyPath);
        }
    }

    private interface CommandReader {
        List<String> readCommand() throws InterruptShellException;
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

    interface InteractiveLineReader {

        String readLine();

        List<String> parseLine(String line);
    }

    public void run(Function<Map<String, ShellCommandsProvider>, InteractiveLineReader> readerBuilder,
                    Function<Map<String, ShellCommandsProvider>, Terminal> terminalBuilder) throws Exception {
        this.readerBuilder = readerBuilder;
        /**
         * Options read from the shell session
         */
        shellCommander = new JCommander();
        final ShellOptions shellOptions = new ShellOptions();
        shellCommander.addObject(shellOptions);

        final Map<String, ShellCommandsProvider> providersMap = registerProviders(shellCommander, properties);

        reader = readerBuilder.apply(providersMap);
        final Terminal terminal = terminalBuilder.apply(providersMap);
        final Map<String, String> variables = System.getenv();

        CommandReader commandReader;
        CommandsInfo commandsInfo = null;

        boolean isNonInteractiveMode = isNonInteractiveMode();
        if (isNonInteractiveMode) {
            final List<String> lines;
            if (mainOptions.filename != null) {
                lines = Files.readAllLines(Paths.get(mainOptions.filename))
                        .stream()
                        .filter(PulsarShell::filterLine)
                        .collect(Collectors.toList());
            } else if (mainOptions.readFromStdin) {
                try (BufferedReader stdinReader = new BufferedReader(new InputStreamReader(System.in))) {
                    lines = stdinReader.lines().filter(PulsarShell::filterLine).collect(Collectors.toList());
                }
            } else {
                lines = new ArrayList<>();
                try (Scanner scanner = new Scanner(mainOptions.inlineCommand);) {
                    while (scanner.hasNextLine()) {
                        String line = scanner.nextLine().trim();
                        lines.add(line);
                    }
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
                public List<String> readCommand() {
                    if (index == lines.size()) {
                        throw new InterruptShellException();
                    }
                    String command = lines.get(index++).trim();
                    final List<String> words = substituteVariables(reader.parseLine(command), variables);
                    command = words.stream().collect(Collectors.joining(" "));
                    if (finalCommandsInfo != null) {
                        finalCommandsInfo.executingCommand = command;
                    } else {
                        output(String.format("[%d/%d] Executing %s", new Object[]{index,
                                lines.size(), command}), terminal);
                    }
                    return words;
                }
            };
        } else {
            commandReader = () -> {
                try {
                    final String line = reader.readLine().trim();
                    return substituteVariables(reader.parseLine(line), variables);
                } catch (org.jline.reader.UserInterruptException
                        | org.jline.reader.EndOfFileException userInterruptException) {
                    throw new InterruptShellException();
                }
            };
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> quit(terminal)));
        while (true) {
            execState = ExecState.IDLE;
            final List<String> words;
            try {
                words = commandReader.readCommand();
            } catch (InterruptShellException interruptShellException) {
                exit(0);
                return;
            }
            execState = ExecState.RUNNING;
            final String line = words.stream().collect(Collectors.joining(" "));
            if (StringUtils.isBlank(line)) {
                continue;
            }
            if (isQuitCommand(line)) {
                exit(0);
                return;
            }
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
            } catch (InterruptShellException t) {
                // no-op
            } catch (Throwable t) {
                t.printStackTrace(terminal.writer());
            } finally {
                final boolean willExitWithError = mainOptions.failOnError && !commandOk;
                if (commandsInfo != null && !willExitWithError) {
                    commandsInfo.executingCommand = null;
                    commandsInfo.executedCommands.add(new CommandsInfo.ExecutedCommandInfo(line, commandOk));
                    printExecutingCommands(terminal, commandsInfo, true);
                }
                pulsarShellCommandsProvider.cleanupState(properties);

            }
            if (mainOptions.failOnError && !commandOk) {
                exit(1);
                return;
            }
        }
    }

    private boolean isNonInteractiveMode() {
        boolean commandOk = true;
        if (mainOptions.inlineCommand != null) {
            if (mainOptions.readFromStdin
                    || mainOptions.filename != null) {
                commandOk = false;
            }
        } else if (mainOptions.readFromStdin
                && mainOptions.filename != null) {
            commandOk = false;
        }

        if (!commandOk) {
            throw new IllegalArgumentException("Cannot use stdin, -e/--execute-command "
                    + "and -f/--filename option at the same time");
        }
        return mainOptions.filename != null || mainOptions.readFromStdin || mainOptions.inlineCommand != null;
    }

    private void printExecutingCommands(Terminal terminal,
                                        CommandsInfo commandsInfo,
                                        boolean printExecuted) {
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

    private static String createPrompt(String hostname) {
        final String string = (hostname == null ? "pulsar" : hostname) + ">";
        return new AttributedStringBuilder()
                .style(LOG_STYLE)
                .append(string)
                .style(AttributedStyle.DEFAULT)
                .append(" ")
                .toAnsi();
    }

    static List<String> substituteVariables(List<String> line, Map<String, String> vars) {
        return line.stream().map(s -> PulsarShell.substituteVariables(s, vars)).collect(Collectors.toList());
    }

    private static String substituteVariables(String string, Map<String, String> vars) {
        for (Substitutor stringSubstitutor : SUBSTITUTORS) {
            string = stringSubstitutor.replace(string, vars);
        }
        return string;
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
        registerProvider(configShell, commander, providerMap);
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

    private static boolean filterLine(String line) {
        return !StringUtils.isBlank(line) && !line.startsWith("#");
    }

    private static String getHostFromUrl(String url) {
        if (url == null) {
            return null;
        }
        try {
            return URI.create(url).getHost();
        } catch (IllegalArgumentException iea) {
            return null;
        }
    }

}
