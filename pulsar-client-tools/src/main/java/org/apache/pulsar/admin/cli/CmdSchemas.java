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
package org.apache.pulsar.admin.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.pulsar.admin.cli.utils.SchemaExtractor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(description = "Operations about schemas")
public class CmdSchemas extends CmdBase {
    private static final ObjectMapper MAPPER = ObjectMapperFactory.create();

    public CmdSchemas(Supplier<PulsarAdmin> admin) {
        super("schemas", admin);
        addCommand("get", new GetSchema());
        addCommand("delete", new DeleteSchema());
        addCommand("upload", new UploadSchema());
        addCommand("extract", new ExtractSchema());
        addCommand("metadata", new GetSchemaMetadata());
        addCommand("compatibility", new TestCompatibility());
    }

    @Command(description = "Get the schema for a topic")
    private class GetSchema extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = {"-v", "--version"}, description = "version", required = false)
        private Long version;

        @Option(names = {"-a", "--all-version"}, description = "all version", required = false)
        private boolean all = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            if (version != null && all) {
                throw new ParameterException("Only one or neither of --version and --all-version can be specified.");
            }
            if (version == null && !all) {
                System.out.println(getAdmin().schemas().getSchemaInfoWithVersion(topic));
            } else if (!all) {
                if (version < 0) {
                    throw new ParameterException("Option --version must be greater than 0, but found " + version);
                }
                System.out.println(getAdmin().schemas().getSchemaInfo(topic, version));
            } else {
                print(getAdmin().schemas().getAllSchemas(topic));
            }
        }
    }

    @Command(description = "Get the schema for a topic")
    private class GetSchemaMetadata extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            print(getAdmin().schemas().getSchemaMetadata(topic));
        }
    }

    @Command(description = "Delete all versions schema of a topic")
    private class DeleteSchema extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-f",
                "--force" }, description = "whether to delete schema completely. If true, delete "
                + "all resources (including metastore and ledger), otherwise only do a mark deletion"
                + " and not remove any resources indeed")
        private boolean force = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            getAdmin().schemas().deleteSchema(topic, force);
        }
    }

    @Command(description = "Update the schema for a topic")
    private class UploadSchema extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-f", "--filename" }, description = "filename", required = true)
        private String schemaFileName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            Path schemaPath = Path.of(schemaFileName);
            File schemaFile = schemaPath.toFile();
            if (!schemaFile.exists()) {
                final StringBuilder sb = new StringBuilder();
                sb.append("Schema file ").append(schemaPath).append(" is not found.");
                if (!schemaPath.isAbsolute()) {
                    sb.append(" Relative path ").append(schemaPath)
                            .append(" is resolved to ").append(schemaPath.toAbsolutePath())
                            .append(". Try to use absolute path if the relative one resolved wrongly.");
                }
                throw new FileNotFoundException(sb.toString());
            }
            PostSchemaPayload input = MAPPER.readValue(schemaFile, PostSchemaPayload.class);
            getAdmin().schemas().createSchema(topic, input);
        }
    }

    @Command(description = "Provide the schema via a topic")
    private class ExtractSchema extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-j", "--jar" }, description = "jar filepath", required = true)
        private String jarFilePath;

        @Option(names = { "-t", "--type" }, description = "type avro or json", required = true)
        private String type;

        @Option(names = { "-c", "--classname" }, description = "class name of pojo", required = true)
        private String className;

        @Option(names = {"-a", "--always-allow-null"}, arity = "1",
                   description = "set schema whether always allow null or not")
        private boolean alwaysAllowNull = true;

        @Option(names = { "-n", "--dry-run"},
                   description = "dost not apply to schema registry, just prints the post schema payload")
        private boolean dryRun = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);

            File file  = new File(jarFilePath);
            ClassLoader cl = new URLClassLoader(new URL[]{ file.toURI().toURL() });
            Class cls = cl.loadClass(className);

            PostSchemaPayload input = new PostSchemaPayload();
            SchemaDefinition<Object> schemaDefinition =
                    SchemaDefinition.builder()
                                    .withPojo(cls)
                                    .withAlwaysAllowNull(alwaysAllowNull)
                                    .build();
            if (type.equalsIgnoreCase("avro")) {
                input.setType("AVRO");
                input.setSchema(SchemaExtractor.getAvroSchemaInfo(schemaDefinition));
            } else if (type.equalsIgnoreCase("json")){
                input.setType("JSON");
                input.setSchema(SchemaExtractor.getJsonSchemaInfo(schemaDefinition));
            } else {
                throw new ParameterException("Invalid schema type " + type + ". Valid options are: avro, json");
            }
            input.setProperties(schemaDefinition.getProperties());
            if (dryRun) {
                System.out.println(topic);
                System.out.println(MAPPER.writerWithDefaultPrettyPrinter()
                                         .writeValueAsString(input));
            } else {
                getAdmin().schemas().createSchema(topic, input);
            }
        }
    }

    @Command(description = "Test schema compatibility")
    private class TestCompatibility extends CliCommand {
        @Parameters(description = "persistent://tenant/namespace/topic", arity = "1")
        private String topicName;

        @Option(names = { "-f", "--filename" }, description = "filename", required = true)
        private String schemaFileName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(topicName);
            PostSchemaPayload input = MAPPER.readValue(new File(schemaFileName), PostSchemaPayload.class);
            getAdmin().schemas().testCompatibility(topic, input);
        }
    }

}
