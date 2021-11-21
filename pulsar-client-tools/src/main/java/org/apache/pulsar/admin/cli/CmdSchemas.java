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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.function.Supplier;

import org.apache.pulsar.admin.cli.utils.SchemaExtractor;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;

@Parameters(commandDescription = "Operations about schemas")
public class CmdSchemas extends CmdBase {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public CmdSchemas(Supplier<PulsarAdmin> admin) {
        super("schemas", admin);
        jcommander.addCommand("get", new GetSchema());
        jcommander.addCommand("delete", new DeleteSchema());
        jcommander.addCommand("upload", new UploadSchema());
        jcommander.addCommand("extract", new ExtractSchema());
    }

    @Parameters(commandDescription = "Get the schema for a topic")
    private class GetSchema extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = {"-v", "--version"}, description = "version", required = false)
        private Long version;

        @Parameter(names = {"-a", "--all-version"}, description = "all version", required = false)
        private boolean all = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
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

    @Parameters(commandDescription = "Delete the latest schema for a topic")
    private class DeleteSchema extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            getAdmin().schemas().deleteSchema(topic);
        }
    }

    @Parameters(commandDescription = "Update the schema for a topic")
    private class UploadSchema extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-f", "--filename" }, description = "filename", required = true)
        private String schemaFileName;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);
            PostSchemaPayload input = MAPPER.readValue(new File(schemaFileName), PostSchemaPayload.class);
            getAdmin().schemas().createSchema(topic, input);
        }
    }

    @Parameters(commandDescription = "Provide the schema via a topic")
    private class ExtractSchema extends CliCommand {
        @Parameter(description = "persistent://tenant/namespace/topic", required = true)
        private java.util.List<String> params;

        @Parameter(names = { "-j", "--jar" }, description = "jar filepath", required = true)
        private String jarFilePath;

        @Parameter(names = { "-t", "--type" }, description = "type avro or json", required = true)
        private String type;

        @Parameter(names = { "-c", "--classname" }, description = "class name of pojo", required = true)
        private String className;

        @Parameter(names = { "--always-allow-null" }, arity = 1,
                   description = "set schema whether always allow null or not")
        private boolean alwaysAllowNull = true;

        @Parameter(names = { "-n", "--dry-run"},
                   description = "dost not apply to schema registry, " +
                                 "just prints the post schema payload")
        private boolean dryRun = false;

        @Override
        void run() throws Exception {
            String topic = validateTopicName(params);

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
            }
            else {
                throw new Exception("Unknown schema type specified as type");
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

}
