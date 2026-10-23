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

import static org.apache.pulsar.shell.config.ConfigStore.DEFAULT_CONFIG;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.internal.InnerClassFactory;
import org.apache.pulsar.internal.ShellCommandsProvider;
import org.apache.pulsar.shell.config.ConfigStore;
import org.apache.pulsar.shell.config.ConfigStore.ConfigEntry;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Shell commands to manage shell configurations.
 */
@Command(description = "Manage Pulsar shell configurations.")
public class ConfigShell implements ShellCommandsProvider {

    private static final String LOCAL_FILES_BASE_DIR = System.getProperty("pulsar.shell.working.dir");

    static File resolveLocalFile(String input) {
        return resolveLocalFile(input, LOCAL_FILES_BASE_DIR);
    }

    static File resolveLocalFile(String input, String baseDir) {
        final File file = new File(input);
        if (!file.isAbsolute() && baseDir != null) {
            return new File(baseDir, input);
        }
        return file;
    }

    private interface RunnableWithResult extends Callable<Integer> {
        boolean run() throws Exception;

        // Picocli entrypoint.
        @Override
        default Integer call() throws Exception {
            if (run()) {
                return 0;
            }
            return 1;
        }
    }

    // Must be a public modifier.
    public class ConfigNameCompletionCandidates implements Iterable<String> {
        @SneakyThrows
        @Override
        public Iterator<String> iterator() {
            return pulsarShell.getConfigStore().listConfigs().stream().map(ConfigEntry::getName).iterator();
        }
    }

    static class ConfigFileCompletionCandidates implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            String path = ConfigShell.resolveLocalFile(".").toPath().toString();
            ArrayList<String> strings = new ArrayList<>();
            strings.add(path);
            return strings.iterator();
        }
    }

    private final PulsarShell pulsarShell;
    private final ConfigStore configStore;
    private final ObjectMapper writer = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private final CommandLine commander = new CommandLine(this, new InnerClassFactory(this));

    @Getter
    private String currentConfig;

    public ConfigShell(PulsarShell pulsarShell, String currentConfig) {
        this.configStore = pulsarShell.getConfigStore();
        this.pulsarShell = pulsarShell;
        this.currentConfig = currentConfig;
        commander.addSubcommand("list", new CmdConfigList());
        commander.addSubcommand("create", new CmdConfigCreate());
        commander.addSubcommand("clone", new CmdConfigClone());
        commander.addSubcommand("update", new CmdConfigUpdate());
        commander.addSubcommand("delete", new CmdConfigDelete());
        commander.addSubcommand("use", new CmdConfigUse());
        commander.addSubcommand("view", new CmdConfigView());
        commander.addSubcommand("set-property", new CmdConfigSetProperty());
        commander.addSubcommand("get-property", new CmdConfigGetProperty());
    }

    @Override
    public String getName() {
        return "config";
    }

    @Override
    public String getServiceUrl() {
        return null;
    }

    @Override
    public String getAdminUrl() {
        return null;
    }

    @Override
    public CommandLine getCommander() {
        return commander;
    }

    @Command(description = "List configurations")
    private class CmdConfigList implements RunnableWithResult {

        @Override
        @SneakyThrows
        public boolean run() {
            print(configStore
                    .listConfigs()
                    .stream()
                    .map(e -> formatEntry(e))
                    .collect(Collectors.toList())
            );
            return true;
        }

        private String formatEntry(ConfigStore.ConfigEntry entry) {
            final String name = entry.getName();
            if (name.equals(currentConfig)) {
                return name + " (*)";
            }
            return name;
        }
    }

    @Command(description = "Use the configuration for next commands")
    private class CmdConfigUse implements RunnableWithResult {
        @Parameters(description = "Name of the config", arity = "1",
                completionCandidates = ConfigNameCompletionCandidates.class)
        private String name;

        @Override
        @SneakyThrows
        public boolean run() {
            final ConfigStore.ConfigEntry config = configStore.getConfig(name);
            if (config == null) {
                print("Config " + name + " not found");
                return false;
            }
            final String value = config.getValue();
            currentConfig = name;
            final Properties properties = new Properties();
            properties.load(new StringReader(value));
            pulsarShell.reload(properties);
            configStore.setLastUsed(name);
            return true;
        }
    }

    @Command(description = "View configuration")
    private class CmdConfigView implements RunnableWithResult {
        @Parameters(description = "Name of the config", arity = "1",
                completionCandidates = ConfigNameCompletionCandidates.class)
        private String name;

        @Override
        @SneakyThrows
        public boolean run() {
            final ConfigStore.ConfigEntry config = configStore.getConfig(this.name);
            if (config == null) {
                print("Config " + name + " not found");
                return false;
            }
            print(config.getValue());
            return true;
        }
    }

    @Command(description = "Delete a configuration")
    private class CmdConfigDelete implements RunnableWithResult {
        @Parameters(description = "Name of the config", arity = "1",
                completionCandidates = ConfigNameCompletionCandidates.class)
        private String name;

        @Override
        @SneakyThrows
        public boolean run() {
            if (DEFAULT_CONFIG.equals(name)) {
                print("'" + name + "' can't be deleted.");
                return false;
            }
            if (currentConfig != null && currentConfig.equals(name)) {
                print("'" + name + "' is currently used and it can't be deleted.");
                return false;
            }
            configStore.deleteConfig(name);
            return true;
        }
    }

    @Command(name = "create", description = "Create a new configuration.")
    private class CmdConfigCreate extends CmdConfigPut {

        @Parameters(description = "Configuration name", arity = "1")
        protected String name;

        @Override
        @SneakyThrows
        boolean verifyCondition() {
            final boolean exists = configStore.getConfig(name) != null;
            if (exists) {
                print("Config '" + name + "' already exists.");
                return false;
            }
            return true;
        }

        @Override
        String name() {
            return name;
        }
    }

    @Command(description = "Update an existing configuration.")
    private class CmdConfigUpdate extends CmdConfigPut {

        @Parameters(description = "Name of the config", arity = "1",
                completionCandidates = ConfigNameCompletionCandidates.class)
        protected String name;

        @Override
        @SneakyThrows
        boolean verifyCondition() {
            if (DEFAULT_CONFIG.equals(name)) {
                print("'" + name + "' can't be updated.");
                return false;
            }
            final boolean exists = configStore.getConfig(name) != null;
            if (!exists) {
                print("Config '" + name + "' does not exist.");
                return false;
            }
            return true;
        }

        @Override
        String name() {
            return name;
        }
    }

    private abstract class CmdConfigPut implements RunnableWithResult {

        @Option(names = {"--url"}, description = "URL of the config")
        protected String url;

        @Option(names = {"--file"}, description = "File path of the config",
                completionCandidates = ConfigFileCompletionCandidates.class)
        protected String file;

        @Option(names = {"--value"}, description = "Inline value of the config")
        protected String inlineValue;

        @Override
        @SneakyThrows
        public boolean run() {
            if (!verifyCondition()) {
                return false;
            }
            final String name = name();
            final String value;
            if (inlineValue != null) {
                if (inlineValue.startsWith("base64:")) {
                    final byte[] bytes = Base64.getDecoder().decode(inlineValue.substring("base64:".length()));
                    value = new String(bytes, StandardCharsets.UTF_8);
                } else {
                    value = inlineValue;
                }
            } else if (file != null) {
                final File f = resolveLocalFile(file);
                if (!f.exists()) {
                    print("File " + f.getAbsolutePath() + " not found.");
                    return false;
                }
                value = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            } else if (url != null) {
                final ByteArrayOutputStream bout = new ByteArrayOutputStream();
                try {
                    try (InputStream in = URI.create(url).toURL().openStream()) {
                        IOUtils.copy(in, bout);
                    }
                } catch (IOException | IllegalArgumentException e) {
                    print("Failed to download configuration: " + e.getMessage());
                    return false;
                }
                value = new String(bout.toByteArray(), StandardCharsets.UTF_8);
            } else {
                print("At least one between --file, --url or --value is required.");
                return false;
            }

            final ConfigStore.ConfigEntry entry = new ConfigStore.ConfigEntry(name, value);
            configStore.putConfig(entry);
            reloadIfCurrent(entry);
            return true;
        }

        abstract String name();

        abstract boolean verifyCondition();
    }


    @Command
    private class CmdConfigClone implements RunnableWithResult {

        @Parameters(description = "Configuration to clone", arity = "1",
                completionCandidates = ConfigNameCompletionCandidates.class)
        protected String cloneFrom;

        @Option(names = {"--name"}, description = "Name of the new config", required = true)
        protected String newName;

        @Override
        @SneakyThrows
        public boolean run() {
            if (DEFAULT_CONFIG.equals(newName) || configStore.getConfig(newName) != null) {
                print("'" + newName + "' already exists.");
                return false;
            }
            final ConfigStore.ConfigEntry config = configStore.getConfig(cloneFrom);
            if (config == null) {
                print("Config '" + config + "' does not exist.");
                return false;
            }

            final ConfigStore.ConfigEntry entry = new ConfigStore.ConfigEntry(newName, config.getValue());
            configStore.putConfig(entry);
            reloadIfCurrent(entry);
            return true;
        }
    }

    private void reloadIfCurrent(ConfigStore.ConfigEntry entry) throws Exception {
        if (currentConfig.equals(entry.getName())) {
            final Properties properties = new Properties();
            properties.load(new StringReader(entry.getValue()));
            pulsarShell.reload(properties);
        }
    }


    @Command(description = "Set a configuration property by name")
    private class CmdConfigSetProperty implements RunnableWithResult {

        @Parameters(description = "Name of the config", arity = "1",
                completionCandidates = ConfigNameCompletionCandidates.class)
        private String name;

        @Option(names = {"-p", "--property"}, required = true, description = "Name of the property to update")
        protected String propertyName;

        @Option(names = {"-v", "--value"}, description = "New value for the property")
        protected String propertyValue;

        @Override
        @SneakyThrows
        public boolean run() {
            if (StringUtils.isBlank(propertyName)) {
                print("-p parameter is required");
                return false;
            }

            if (propertyValue == null) {
                print("-v parameter is required. You can pass an empty value to empty the property. (-v= )");
                return false;
            }


            final ConfigStore.ConfigEntry config = configStore.getConfig(this.name);
            if (config == null) {
                print("Config " + name + " not found");
                return false;
            }
            ConfigStore.setProperty(config, propertyName, propertyValue);
            print("Property " + propertyName + " set for config " + name);
            configStore.putConfig(config);
            reloadIfCurrent(config);
            return true;
        }
    }

    @Command(description = "Get a configuration property by name")
    private class CmdConfigGetProperty implements RunnableWithResult {

        @Parameters(description = "Name of the config", arity = "1",
                completionCandidates = ConfigNameCompletionCandidates.class)
        private String name;

        @Option(names = {"-p", "--property"}, required = true, description = "Name of the property")
        protected String propertyName;

        @Override
        @SneakyThrows
        public boolean run() {
            if (StringUtils.isBlank(propertyName)) {
                print("-p parameter is required");
                return false;
            }

            final ConfigStore.ConfigEntry config = configStore.getConfig(this.name);
            if (config == null) {
                print("Config " + name + " not found");
                return false;
            }
            final String value = ConfigStore.getProperty(config, propertyName);
            if (!StringUtils.isBlank(value)) {
                print(value);
            }
            return true;
        }
    }


    <T> void print(List<T> items) {
        for (T item : items) {
            print(item);
        }
    }

    <T> void print(T item) {
        try {
            if (item instanceof String) {
                commander.getOut().println((String) item);
            } else {
                commander.getOut().println(writer.writeValueAsString(item));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}