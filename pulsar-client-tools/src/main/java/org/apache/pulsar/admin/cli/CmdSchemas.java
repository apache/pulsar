/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.admin.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

@Parameters(commandDescription = "Operations about schemas")
public class CmdSchemas extends CmdBase {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public CmdSchemas(PulsarAdmin admin) {
        super("schemas", admin);
    }

    @Parameters(commandDescription = "Get the latest schema")
    private class GetSchema extends CliCommand {
        @Parameter(description = "tenant", required = true)
        private String tennant;

        @Parameter(description = "namespace", required = true)
        private String namespace;

        @Parameter(description = "schema", required = true)
        private String topic;

        @Parameter(description = "version", required = false)
        private Long version;

        @Override
        void run() throws Exception {
            if (version == null) {
                print(admin.getSchemas().getSchemaInfo(tennant, namespace, topic));
            } else {
                print(admin.getSchemas().getSchemaInfo(tennant, namespace, topic, version));
            }
        }
    }

    @Parameters(commandDescription = "Delete the latest schema")
    private class DeleteSchema extends CliCommand {
        @Parameter(description = "tenant", required = true)
        private String tennant;

        @Parameter(description = "namespace", required = true)
        private String namespace;

        @Parameter(description = "schema", required = true)
        private String topic;

        @Override
        void run() throws Exception {
            admin.getSchemas().deleteSchema(tennant, namespace, topic);
        }
    }

    private class UploadSchema extends CliCommand {
        @Parameter(description = "tenant", required = true)
        private String tennant;

        @Parameter(description = "namespace", required = true)
        private String namespace;

        @Parameter(description = "schema", required = true)
        private String topic;

        @Parameter(description = "filename", required = true)
        private String schemaFileName;

        @Override
        void run() throws Exception {
            PostSchemaPayload input = MAPPER.readValue(new File(schemaFileName), PostSchemaPayload.class);
            SchemaInfo info = new SchemaInfo();
            info.setProperties(input.getProperties());
            info.setType(SchemaType.valueOf(input.getType()));
            info.setSchema(input.getSchema().getBytes());
            admin.getSchemas().createSchema(tennant, namespace, topic, info);
        }
    }

}