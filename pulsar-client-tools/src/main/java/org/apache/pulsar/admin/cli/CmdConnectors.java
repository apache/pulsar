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
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.StringConverter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.functions.shaded.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.SinkConfig;
import org.apache.pulsar.functions.utils.SourceConfig;

import java.io.File;
import java.io.IOException;

@Slf4j
@Getter
@Parameters(commandDescription = "Interface for managing Pulsar Connectors (Ingress and egress data to and from " +
        "Pulsar)")
public class CmdConnectors extends CmdBase {

    private final CreateSource createSource;
    private final CreateSink createSink;
    private final DeleteConnector deleteConnector;

    public CmdConnectors(PulsarAdmin admin) {
        super("connectors", admin);
        createSource = new CreateSource();
        createSink = new CreateSink();
        deleteConnector = new DeleteConnector();
        jcommander.addCommand("create-source", createSource);
        jcommander.addCommand("create-sink", createSink);
        jcommander.addCommand("delete", deleteConnector);
    }

    /**
     * Base command
     */
    @Getter
    abstract class BaseCommand extends CliCommand {
        @Override
        void run() throws Exception {
            processArguments();
            runCmd();
        }

        void processArguments() throws Exception {
        }

        abstract void runCmd() throws Exception;
    }

    @Parameters(commandDescription = "Create Pulsar source connectors")
    class CreateSource extends BaseCommand {
        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;
        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The function's name")
        protected String name;
        @Parameter(names = "--className", description = "The function's class name")
        protected String className;
        @Parameter(names = "--topicName", description = "Pulsar topic to ingress data to")
        protected String topicName;
        @Parameter(names = "--serdeClassName", description = "")
        protected String serdeClassName;
        @Parameter(names = "--parallelism", description = "")
        protected String parallelism;
        @Parameter(
                names = "--jar",
                description = "Path to the jar file for the function (if the function is written in Java)",
                listConverter = StringConverter.class)
        protected String jarFile;

        @Parameter(names = "--functionConfigFile", description = "The path to a YAML config file specifying the "
                + "function's configuration")
        protected String fnConfigFile;

        protected SourceConfig sourceConfig;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            if (null != fnConfigFile) {
                this.sourceConfig = loadSourceConfig(fnConfigFile);
            } else {
                this.sourceConfig = new SourceConfig();
            }

            if (null != fqfn) {
                parseFullyQualifiedFunctionName(fqfn, sourceConfig);
            } else {
                if (null != tenant) {
                    sourceConfig.setTenant(tenant);
                }
                if (null != namespace) {
                    sourceConfig.setNamespace(namespace);
                }
                if (null != name) {
                    sourceConfig.setName(name);
                }
            }
            if (null != className) {
                this.sourceConfig.setClassName(className);
            }
            if (null != topicName) {
                sourceConfig.setTopicName(topicName);
            }
            if (null != serdeClassName) {
                sourceConfig.setSerdeClassName(serdeClassName);
            }

            if (parallelism == null) {
                if (sourceConfig.getParallelism() == 0) {
                    sourceConfig.setParallelism(1);
                }
            } else {
                int num = Integer.parseInt(parallelism);
                if (num <= 0) {
                    throw new IllegalArgumentException("The parallelism factor (the number of instances) for the "
                            + "connector must be positive");
                }
                sourceConfig.setParallelism(num);
            }

            if (null == jarFile) {
                throw new IllegalArgumentException("Connector JAR not specfied");
            }
        }

        @Override
        void runCmd() throws Exception {
            if (!areAllRequiredFieldsPresentForSource(sourceConfig)) {
                throw new RuntimeException("Missing arguments");
            }
            admin.functions().createFunction(convertSourceConfig(sourceConfig), jarFile);
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Create Pulsar sink connectors")
    class CreateSink extends BaseCommand {

        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;
        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The function's name")
        protected String name;
        @Parameter(names = "--className", description = "The function's class name")
        protected String className;
        @Parameter(names = "--topicName", description = "Pulsar topic to egress data from")
        protected String topicName;
        @Parameter(names = "--serdeClassName", description = "")
        protected String serdeClassName;
        @Parameter(names = "--parallelism", description = "")
        protected String parallelism;
        @Parameter(
                names = "--jar",
                description = "Path to the jar file for the function (if the function is written in Java)",
                listConverter = StringConverter.class)
        protected String jarFile;

        @Parameter(names = "--functionConfigFile", description = "The path to a YAML config file specifying the "
                + "function's configuration")
        protected String fnConfigFile;

        protected SinkConfig sinkConfig;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            if (null != fnConfigFile) {
                this.sinkConfig = loadSinkConfig(fnConfigFile);
            } else {
                this.sinkConfig = new SinkConfig();
            }

            if (null != fqfn) {
                parseFullyQualifiedFunctionName(fqfn, sinkConfig);
            } else {
                if (null != tenant) {
                    sinkConfig.setTenant(tenant);
                }
                if (null != namespace) {
                    sinkConfig.setNamespace(namespace);
                }
                if (null != name) {
                    sinkConfig.setName(name);
                }
            }
            if (null != className) {
                this.sinkConfig.setClassName(className);
            }
            if (null != topicName) {
                sinkConfig.setTopicName(topicName);
            }
            if (null != serdeClassName) {
                sinkConfig.setSerdeClassName(serdeClassName);
            }

            if (parallelism == null) {
                if (sinkConfig.getParallelism() == 0) {
                    sinkConfig.setParallelism(1);
                }
            } else {
                int num = Integer.parseInt(parallelism);
                if (num <= 0) {
                    throw new IllegalArgumentException("The parallelism factor (the number of instances) for the "
                            + "connector must be positive");
                }
                sinkConfig.setParallelism(num);
            }

            if (null == jarFile) {
                throw new IllegalArgumentException("Connector JAR not specfied");
            }
        }

        @Override
        void runCmd() throws Exception {
            if (!areAllRequiredFieldsPresentForSink(sinkConfig)) {
                throw new RuntimeException("Missing arguments");
            }
            admin.functions().createFunction(convertSinkConfig(sinkConfig), jarFile);
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Stops a Pulsar connector")
    class DeleteConnector extends BaseCommand {

        @Parameter(names = "--fqfn", description = "The Fully Qualified Function Name (FQFN) for the function")
        protected String fqfn;

        @Parameter(names = "--tenant", description = "The function's tenant")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The function's namespace")
        protected String namespace;

        @Parameter(names = "--name", description = "The function's name")
        protected String name;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            boolean usesSetters = (null != tenant || null != namespace || null != name);
            boolean usesFqfn = (null != fqfn);

            // Throw an exception if --fqfn is set alongside any combination of --tenant, --namespace, and --name
            if (usesFqfn && usesSetters) {
                throw new RuntimeException(
                        "You must specify either a Fully Qualified Function Name (FQFN) or tenant, namespace, and " +
                                "function name");
            } else if (usesFqfn) {
                // If the --fqfn flag is used, parse tenant, namespace, and name using that flag
                String[] fqfnParts = fqfn.split("/");
                if (fqfnParts.length != 3) {
                    throw new RuntimeException(
                            "Fully qualified function names (FQFNs) must be of the form tenant/namespace/name");
                }
                tenant = fqfnParts[0];
                namespace = fqfnParts[1];
                name = fqfnParts[2];
            } else {
                if (null == tenant || null == namespace || null == name) {
                    throw new RuntimeException(
                            "You must specify a tenant, namespace, and name for the function or a Fully Qualified " +
                                    "Function Name (FQFN)");
                }
            }
        }

        @Override
        void runCmd() throws Exception {
            admin.functions().deleteFunction(tenant, namespace, name);
            print("Deleted successfully");
        }
    }

    private FunctionDetails convertSourceConfig(SourceConfig sourceConfig) {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (sourceConfig.getTenant() != null) {
            functionDetailsBuilder.setTenant(sourceConfig.getTenant());
        }
        if (sourceConfig.getNamespace() != null) {
            functionDetailsBuilder.setNamespace(sourceConfig.getNamespace());
        }
        if (sourceConfig.getName() != null) {
            functionDetailsBuilder.setName(sourceConfig.getName());
        }
        if (sourceConfig.getClassName() != null) {
            functionDetailsBuilder.setClassName(sourceConfig.getClassName());
        }
        if (sourceConfig.getTopicName() != null) {
            functionDetailsBuilder.setOutput(sourceConfig.getTopicName());
        }
        if (sourceConfig.getSerdeClassName() != null) {
            functionDetailsBuilder.setOutputSerdeClassName(sourceConfig.getSerdeClassName());
        }
        // connectors only support Java runtime for now
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.putAllUserConfig(sourceConfig.getConfigs());
        functionDetailsBuilder.setParallelism(sourceConfig.getParallelism());
        functionDetailsBuilder.setAutoAck(false);
        return functionDetailsBuilder.build();
    }

    private FunctionDetails convertSinkConfig(SinkConfig sinkConfig) {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
        if (sinkConfig.getTenant() != null) {
            functionDetailsBuilder.setTenant(sinkConfig.getTenant());
        }
        if (sinkConfig.getNamespace() != null) {
            functionDetailsBuilder.setNamespace(sinkConfig.getNamespace());
        }
        if (sinkConfig.getName() != null) {
            functionDetailsBuilder.setName(sinkConfig.getName());
        }
        if (sinkConfig.getClassName() != null) {
            functionDetailsBuilder.setClassName(sinkConfig.getClassName());
        }
        if (sinkConfig.getTopicName() != null) {
            functionDetailsBuilder.addInputs(sinkConfig.getTopicName());
        }
        if (sinkConfig.getSerdeClassName() != null) {
            functionDetailsBuilder.setOutputSerdeClassName(sinkConfig.getSerdeClassName());
        }
        // connectors only support Java runtime for now
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.putAllUserConfig(sinkConfig.getConfigs());
        functionDetailsBuilder.setParallelism(sinkConfig.getParallelism());
        functionDetailsBuilder.setAutoAck(false);
        return functionDetailsBuilder.build();
    }

    private static SinkConfig loadSinkConfig(String file) throws IOException {
        return (SinkConfig) loadConfig(file, SinkConfig.class);
    }

    private static SourceConfig loadSourceConfig(String file) throws IOException {
        return (SourceConfig) loadConfig(file, SourceConfig.class);
    }

    private static Object loadConfig(String file, Class<?> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(file), clazz);
    }

    private void parseFullyQualifiedFunctionName(String fqfn, SourceConfig sourceConfig) {
        String[] args = fqfn.split("/");
        if (args.length != 3) {
            throw new RuntimeException("Fully qualified function names (FQFNs) must be of the form "
                    + "tenant/namespace/name");
        } else {
            sourceConfig.setTenant(args[0]);
            sourceConfig.setNamespace(args[1]);
            sourceConfig.setName(args[2]);
        }
    }

    private void parseFullyQualifiedFunctionName(String fqfn, SinkConfig SinkConfig) {
        String[] args = fqfn.split("/");
        if (args.length != 3) {
            throw new RuntimeException("Fully qualified function names (FQFNs) must be of the form "
                    + "tenant/namespace/name");
        } else {
            SinkConfig.setTenant(args[0]);
            SinkConfig.setNamespace(args[1]);
            SinkConfig.setName(args[2]);
        }
    }

    public static boolean areAllRequiredFieldsPresentForSource(SourceConfig sourceConfig) {
        return sourceConfig.getTenant() != null && sourceConfig.getNamespace() != null
                && sourceConfig.getName() != null && sourceConfig.getClassName() != null
                && sourceConfig.getTopicName() != null || sourceConfig.getSerdeClassName() != null
                && sourceConfig.getParallelism() > 0;
    }

    public static boolean areAllRequiredFieldsPresentForSink(SinkConfig sinkConfig) {
        return sinkConfig.getTenant() != null && sinkConfig.getNamespace() != null
                && sinkConfig.getName() != null && sinkConfig.getClassName() != null
                && sinkConfig.getTopicName() != null || sinkConfig.getSerdeClassName() != null
                && sinkConfig.getParallelism() > 0;
    }
}
