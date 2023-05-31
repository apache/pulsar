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
package org.apache.pulsar.io.docs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

@Slf4j
public class ConnectorDocGenerator {

    private static final String INDENT = "  ";

    private static Reflections newReflections() throws Exception {
        final String[] classpathList = System.getProperty("java.class.path").split(":");
        final List<URL> urlList = new ArrayList<>();
        for (String file : classpathList) {
            urlList.add(new File(file).toURI().toURL());
        }
        return new Reflections(new ConfigurationBuilder().setUrls(urlList));
    }

    private final Reflections reflections;

    public ConnectorDocGenerator() throws Exception {
        this.reflections = newReflections();
    }

    private void generateConnectorYamlFile(Class<?> configClass, PrintWriter writer) {
        log.info("Processing connector config class : {}", configClass);

        writer.println("configs:");

        Field[] fields = configClass.getDeclaredFields();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            FieldDoc fieldDoc = field.getDeclaredAnnotation(FieldDoc.class);
            if (null == fieldDoc) {
                final String message = "Missing FieldDoc for field '%s' in class '%s'."
                        .formatted(field.getName(), configClass.getCanonicalName());
                throw new RuntimeException(message);
            }
            writer.println(INDENT + "# " + fieldDoc.help());
            String fieldPrefix = "";
            if (!fieldDoc.required()) {
                fieldPrefix = "# ";
            }
            if (Strings.isNullOrEmpty(fieldDoc.defaultValue())) {
                writer.println(INDENT + fieldPrefix + field.getName() + ":");
            } else {
                writer.println(INDENT + fieldPrefix + field.getName() + ": " + fieldDoc.defaultValue());
            }
            writer.println();
        }
        writer.flush();
    }

    private void generateConnectorYamlFile(Class<?> connectorClass, Connector connectorDef, PrintWriter writer) {
        log.info("Processing connector definition : {}", connectorDef);
        writer.println("# " + connectorDef.type() + " connector : " + connectorClass.getName());
        writer.println();
        writer.println("# " + connectorDef.help());
        writer.println();
        generateConnectorYamlFile(connectorDef.configClass(), writer);
    }

    private void generatorConnectorYamlFiles(String outputDir) throws IOException {
        Set<Class<?>> connectorClasses = reflections.getTypesAnnotatedWith(Connector.class);
        log.info("Retrieve all `Connector` annotated classes : {}", connectorClasses);

        for (Class<?> connectorClass : connectorClasses) {
            final Connector connectorDef = connectorClass.getDeclaredAnnotation(Connector.class);
            final String name = connectorDef.name().toLowerCase();
            final String type = connectorDef.type().name().toLowerCase();
            final String filename = "pulsar-io-%s-%s.yml".formatted(name, type);
            final Path outputPath = Path.of(outputDir, filename);
            try (FileOutputStream fos = new FileOutputStream(outputPath.toFile())) {
                PrintWriter pw = new PrintWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8));
                generateConnectorYamlFile(connectorClass, connectorDef, pw);
                pw.flush();
            }
        }
    }

    /**
     * Args for stats generator.
     */
    private static class MainArgs {
        @Parameter(
                names = {"-o", "--output-dir"},
                description = "The output dir to dump connector docs",
                required = true)
        String outputDir = null;

        @Parameter(names = {"-h", "--help"}, description = "Show this help message")
        boolean help = false;
    }

    public static void main(String[] args) throws Exception {
        MainArgs mainArgs = new MainArgs();

        JCommander commander = new JCommander();
        try {
            commander.setProgramName("connector-doc-gen");
            commander.addObject(mainArgs);
            commander.parse(args);
            if (mainArgs.help) {
                commander.usage();
                Runtime.getRuntime().exit(0);
                return;
            }
        } catch (Exception e) {
            commander.usage();
            Runtime.getRuntime().exit(1);
            return;
        }

        ConnectorDocGenerator docGen = new ConnectorDocGenerator();
        docGen.generatorConnectorYamlFiles(mainArgs.outputDir);
    }

}
