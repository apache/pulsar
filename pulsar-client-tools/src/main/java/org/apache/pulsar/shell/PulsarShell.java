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
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/**
 * Main Pulsar shell class invokable from the pulsar-shell script.
 */
public class PulsarShell {

    private static final String EXIT_MESSAGE = "Goodbye!";
    private static final String PROPERTY_PERSIST_HISTORY_ENABLED = "shellHistoryPersistEnabled";
    private static final String PROPERTY_PERSIST_HISTORY_PATH = "shellHistoryPersistPath";

    static final class MainOptions {
        @Parameter(names = {"-h", "--help"}, help = true, description = "Show this help.")
        boolean help;
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
        new PulsarShell().run(properties);
    }

    public void run(Properties properties) throws Exception {
        final Terminal terminal = TerminalBuilder.builder().build();
        run(properties, (providersMap) -> {
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

    public void run(Properties properties,
                    Function<Map<String, ShellCommandsProvider>, LineReader> readerBuilder,
                    Function<Map<String, ShellCommandsProvider>, Terminal> terminalBuilder) throws Exception {
        System.setProperty("org.jline.terminal.dumb", "true");

        final JCommander mainCommander = new JCommander();
        final MainOptions mainOptions = new MainOptions();
        mainCommander.addObject(mainOptions);

        final Map<String, ShellCommandsProvider> providersMap = registerProviders(mainCommander, properties);

        final LineReader reader = readerBuilder.apply(providersMap);
        final Terminal terminal = terminalBuilder.apply(providersMap);
        final String prompt = createPrompt();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> quit(terminal)));
        while (true) {
            String line;
            try {
                line = reader.readLine(prompt).trim();
            } catch (org.jline.reader.UserInterruptException userInterruptException) {
                break;
            }
            if (line.isBlank()) {
                continue;
            }
            if (isQuitCommand(line)) {
                break;
            }
            final List<String> words = parseLine(reader, line);

            if (mainOptions.help) {
                mainCommander.usage();
                continue;
            }

            final ShellCommandsProvider pulsarShellCommandsProvider = getProviderFromArgs(mainCommander, words);
            if (pulsarShellCommandsProvider == null) {
                mainCommander.usage();
                continue;
            }
            String[] argv = extractAndConvertArgs(words);
            try {
                pulsarShellCommandsProvider.runCommand(argv);
            } catch (Throwable t) {
                t.printStackTrace(terminal.writer());
            } finally {
                pulsarShellCommandsProvider.cleanupState(properties);
            }
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
                .style(AttributedStyle.DEFAULT.foreground(25, 143, 255).background(230, 241, 255))
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

}
