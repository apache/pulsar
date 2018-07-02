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
import com.beust.jcommander.converters.StringConverter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Getter;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.admin.cli.utils.CmdUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.FunctionsImpl;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.Function.Resources;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.functions.utils.SourceConfig;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.functions.utils.validation.ConfigValidation;
import org.apache.pulsar.functions.utils.validation.ValidatorImpls.ImplementsClassValidator;
import org.apache.pulsar.io.core.Source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.util.Map;

import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.Utils.fileExists;
import static org.apache.pulsar.functions.utils.Utils.getSourceType;
import static org.apache.pulsar.functions.worker.Utils.downloadFromHttpUrl;

@Getter
@Parameters(commandDescription = "Interface for managing Pulsar IO Sources (ingress data into Pulsar)")
public class CmdSources extends CmdBase {

    private final CreateSource createSource;
    private final DeleteSource deleteSource;
    private final UpdateSource updateSource;
    private final LocalSourceRunner localSourceRunner;

    public CmdSources(PulsarAdmin admin) {
        super("source", admin);
        createSource = new CreateSource();
        updateSource = new UpdateSource();
        deleteSource = new DeleteSource();
        localSourceRunner = new LocalSourceRunner();

        jcommander.addCommand("create", createSource);
        jcommander.addCommand("update", updateSource);
        jcommander.addCommand("delete", deleteSource);
        jcommander.addCommand("localrun", localSourceRunner);
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

    @Parameters(commandDescription = "Run a Pulsar IO source connector locally (rather than deploying it to the Pulsar cluster)")
    class LocalSourceRunner extends CreateSource {

        @Parameter(names = "--brokerServiceUrl", description = "The URL for the Pulsar broker")
        protected String brokerServiceUrl;
        
        @Parameter(names = "--clientAuthPlugin", description = "Client authentication plugin using which function-process can connect to broker")
        protected String clientAuthPlugin;
        
        @Parameter(names = "--clientAuthParams", description = "Client authentication param")
        protected String clientAuthParams;
        
        @Parameter(names = "--use_tls", description = "Use tls connection\n")
        protected boolean useTls;

        @Parameter(names = "--tls_allow_insecure", description = "Allow insecure tls connection\n")
        protected boolean tlsAllowInsecureConnection;
        
        @Parameter(names = "--hostname_verification_enabled", description = "Enable hostname verification")
        protected boolean tlsHostNameVerificationEnabled;
        
        @Parameter(names = "--tls_trust_cert_path", description = "tls trust cert file path")
        protected String tlsTrustCertFilePath;

        @Override
        void runCmd() throws Exception {
            CmdFunctions.startLocalRun(createSourceConfigProto2(sourceConfig), sourceConfig.getParallelism(),
                    0, brokerServiceUrl, null,
                    AuthenticationConfig.builder().clientAuthenticationPlugin(clientAuthPlugin)
                            .clientAuthenticationParameters(clientAuthParams).useTls(useTls)
                            .tlsAllowInsecureConnection(tlsAllowInsecureConnection)
                            .tlsHostnameVerificationEnable(tlsHostNameVerificationEnabled)
                            .tlsTrustCertsFilePath(tlsTrustCertFilePath).build(),
                    sourceConfig.getJar(), admin);
        }
    }

    @Parameters(commandDescription = "Submit a Pulsar IO source connector to run in a Pulsar cluster")
    public class CreateSource extends SourceCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(this.sourceConfig.getJar())) {
                admin.functions().createFunctionWithUrl(createSourceConfig(sourceConfig), sourceConfig.getJar());
            } else {
                admin.functions().createFunction(createSourceConfig(sourceConfig), sourceConfig.getJar());
            }
            print("Created successfully");
        }
    }

    @Parameters(commandDescription = "Update a Pulsar IO source connector")
    public class UpdateSource extends SourceCommand {
        @Override
        void runCmd() throws Exception {
            if (Utils.isFunctionPackageUrlSupported(sourceConfig.getJar())) {
                admin.functions().updateFunctionWithUrl(createSourceConfig(sourceConfig), sourceConfig.getJar());
            } else {
                admin.functions().updateFunction(createSourceConfig(sourceConfig), sourceConfig.getJar());
            }
            print("Updated successfully");
        }
    }

    abstract class SourceCommand extends BaseCommand {
        @Parameter(names = "--tenant", description = "The source's tenant")
        protected String tenant;
        @Parameter(names = "--namespace", description = "The source's namespace")
        protected String namespace;
        @Parameter(names = "--name", description = "The source's name")
        protected String name;
        @Parameter(names = "--processingGuarantees", description = "The processing guarantees (aka delivery semantics) applied to the Source")
        protected FunctionConfig.ProcessingGuarantees processingGuarantees;
        @Parameter(names = "--className", description = "The source's class name")
        protected String className;
        @Parameter(names = "--destinationTopicName", description = "The Pulsar topic to which data is sent")
        protected String destinationTopicName;
        @Parameter(names = "--deserializationClassName", description = "The SerDe classname for the source")
        protected String deserializationClassName;
        @Parameter(names = "--parallelism", description = "The source's parallelism factor (i.e. the number of source instances to run)")
        protected Integer parallelism;
        @Parameter(names = "--jar", description = "The path to the jar file for the Source. It also supports url-path [http/https/file (file protocol assumes that file already exists on worker host)] from which worker can download the package.", listConverter = StringConverter.class)
        protected String jarFile;
        @Parameter(names = "--sourceConfigFile", description = "The path to a YAML config file specifying the "
                + "source's configuration")
        protected String sourceConfigFile;
        @Parameter(names = "--cpu", description = "The CPU (in cores) that needs to be allocated per source instance (applicable only to Docker runtime)")
        protected Double cpu;
        @Parameter(names = "--ram", description = "The RAM (in bytes) that need to be allocated per source instance (applicable only to the process and Docker runtimes)")
        protected Long ram;
        @Parameter(names = "--disk", description = "The disk (in bytes) that need to be allocated per source instance (applicable only to Docker runtime)")
        protected Long disk;
        @Parameter(names = "--sourceConfig", description = "Source config key/values")
        protected String sourceConfigString;

        protected SourceConfig sourceConfig;

        @Override
        void processArguments() throws Exception {
            super.processArguments();

            if (null != sourceConfigFile) {
                this.sourceConfig = CmdUtils.loadConfig(sourceConfigFile, SourceConfig.class);
            } else {
                this.sourceConfig = new SourceConfig();
            }
            if (null != tenant) {
                sourceConfig.setTenant(tenant);
            }
            if (null != namespace) {
                sourceConfig.setNamespace(namespace);
            }
            if (null != name) {
                sourceConfig.setName(name);
            }
            if (null != className) {
                this.sourceConfig.setClassName(className);
            }
            if (null != destinationTopicName) {
                sourceConfig.setTopicName(destinationTopicName);
            }
            if (null != deserializationClassName) {
                sourceConfig.setSerdeClassName(deserializationClassName);
            }
            if (null != processingGuarantees) {
                sourceConfig.setProcessingGuarantees(processingGuarantees);
            }
            if (parallelism != null) {
                sourceConfig.setParallelism(parallelism);
            }

            if (jarFile != null) {
                sourceConfig.setJar(jarFile);
            }
            
            sourceConfig.setResources(new org.apache.pulsar.functions.utils.Resources(cpu, ram, disk));

            if (null != sourceConfigString) {
                Type type = new TypeToken<Map<String, String>>(){}.getType();
                Map<String, Object> sourceConfigMap = new Gson().fromJson(sourceConfigString, type);
                sourceConfig.setConfigs(sourceConfigMap);
            }

            inferMissingArguments(sourceConfig);
        }

        private void inferMissingArguments(SourceConfig sourceConfig) {
            if (sourceConfig.getTenant() == null) {
                sourceConfig.setTenant(PUBLIC_TENANT);
            }
            if (sourceConfig.getNamespace() == null) {
                sourceConfig.setNamespace(DEFAULT_NAMESPACE);
            }
        }

        protected void validateSourceConfigs(SourceConfig sourceConfig) {
            if (StringUtils.isBlank(sourceConfig.getJar())) {
                throw new ParameterException("Source jar not specfied");
            }

            boolean isJarPathUrl = Utils.isFunctionPackageUrlSupported(sourceConfig.getJar());
            
            String jarFilePath = null;
            if (isJarPathUrl) {
                // download jar file if url is http
                if(sourceConfig.getJar().startsWith(Utils.HTTP)) {
                    File tempPkgFile = null;
                    try {
                        tempPkgFile = File.createTempFile(sourceConfig.getName(), "source");
                        downloadFromHttpUrl(sourceConfig.getJar(), new FileOutputStream(tempPkgFile));
                        jarFilePath = tempPkgFile.getAbsolutePath();
                    } catch(Exception e) {
                        if(tempPkgFile!=null ) {
                            tempPkgFile.deleteOnExit();
                        }
                        throw new ParameterException("Failed to download jar from " + sourceConfig.getJar()
                                + ", due to =" + e.getMessage());
                    }
                }
            } else {
                jarFilePath = sourceConfig.getJar();
            }
            
            
            // if jar file is present locally then load jar and validate SinkClass in it
            if (jarFilePath != null) {
                if (!fileExists(jarFilePath)) {
                    throw new ParameterException("Jar file " + jarFilePath + " does not exist");
                }

                File file = new File(jarFilePath);
                ClassLoader userJarLoader;
                try {
                    userJarLoader = Reflections.loadJar(file);
                } catch (MalformedURLException e) {
                    throw new ParameterException("Failed to load user jar " + file + " with error " + e.getMessage());
                }
                // make sure the function class loader is accessible thread-locally
                Thread.currentThread().setContextClassLoader(userJarLoader);

                // jar is already loaded, validate against Source-Class name
                (new ImplementsClassValidator(Source.class)).validateField("className", sourceConfig.getClassName());
            }

            try {
             // Need to load jar and set context class loader before calling
                ConfigValidation.validateConfig(sourceConfig, FunctionConfig.Runtime.JAVA.name());
            } catch (Exception e) {
                throw new ParameterException(e.getMessage());
            }
        }

        protected org.apache.pulsar.functions.proto.Function.FunctionDetails createSourceConfigProto2(SourceConfig sourceConfig)
                throws IOException {
            org.apache.pulsar.functions.proto.Function.FunctionDetails.Builder functionDetailsBuilder
                    = org.apache.pulsar.functions.proto.Function.FunctionDetails.newBuilder();
            Utils.mergeJson(FunctionsImpl.printJson(createSourceConfig(sourceConfig)), functionDetailsBuilder);
            return functionDetailsBuilder.build();
        }

        protected FunctionDetails createSourceConfig(SourceConfig sourceConfig) {

            // check if source configs are valid
            validateSourceConfigs(sourceConfig);

            String typeArg = sourceConfig.getJar().startsWith(Utils.FILE) ? null
                    : getSourceType(sourceConfig.getClassName()).getName();

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
            functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
            functionDetailsBuilder.setParallelism(sourceConfig.getParallelism());
            functionDetailsBuilder.setClassName(IdentityFunction.class.getName());
            functionDetailsBuilder.setAutoAck(true);
            if (sourceConfig.getProcessingGuarantees() != null) {
                functionDetailsBuilder.setProcessingGuarantees(
                        convertProcessingGuarantee(sourceConfig.getProcessingGuarantees()));
            }

            // set source spec
            SourceSpec.Builder sourceSpecBuilder = SourceSpec.newBuilder();
            sourceSpecBuilder.setClassName(sourceConfig.getClassName());
            if (sourceConfig.getConfigs() != null) {
                sourceSpecBuilder.setConfigs(new Gson().toJson(sourceConfig.getConfigs()));
            }
            if (typeArg != null) {
                sourceSpecBuilder.setTypeClassName(typeArg);
            }
            functionDetailsBuilder.setSource(sourceSpecBuilder);

            // set up sink spec.
            // Sink spec classname should be empty so that the default pulsar sink will be used
            SinkSpec.Builder sinkSpecBuilder = SinkSpec.newBuilder();
            if (sourceConfig.getSerdeClassName() != null && !sourceConfig.getSerdeClassName().isEmpty()) {
                sinkSpecBuilder.setSerDeClassName(sourceConfig.getSerdeClassName());
            }
            sinkSpecBuilder.setTopic(sourceConfig.getTopicName());
            
            if (typeArg != null) {
                sinkSpecBuilder.setTypeClassName(typeArg);
            }

            functionDetailsBuilder.setSink(sinkSpecBuilder);

            if (sourceConfig.getResources() != null) {
                Resources.Builder bldr = Resources.newBuilder();
                if (sourceConfig.getResources().getCpu() != null) {
                    bldr.setCpu(sourceConfig.getResources().getCpu());
                }
                if (sourceConfig.getResources().getRam() != null) {
                    bldr.setRam(sourceConfig.getResources().getRam());
                }
                if (sourceConfig.getResources().getDisk() != null) {
                    bldr.setDisk(sourceConfig.getResources().getDisk());
                }
                functionDetailsBuilder.setResources(bldr.build());
            }

            return functionDetailsBuilder.build();
        }
    }

    @Parameters(commandDescription = "Stops a Pulsar IO source connector")
    class DeleteSource extends BaseCommand {

        @Parameter(names = "--tenant", description = "The tenant of a sink or source")
        protected String tenant;

        @Parameter(names = "--namespace", description = "The namespace of a sink or source")
        protected String namespace;

        @Parameter(names = "--name", description = "The name of a sink or source")
        protected String name;

        @Override
        void processArguments() throws Exception {
            super.processArguments();
            if (null == name) {
                throw new RuntimeException(
                        "You must specify a name for the source");
            }
            if (tenant == null) {
                tenant = PUBLIC_TENANT;
            }
            if (namespace == null) {
                namespace = DEFAULT_NAMESPACE;
            }
        }

        @Override
        void runCmd() throws Exception {
            admin.functions().deleteFunction(tenant, namespace, name);
            print("Delete source successfully");
        }
    }
}
