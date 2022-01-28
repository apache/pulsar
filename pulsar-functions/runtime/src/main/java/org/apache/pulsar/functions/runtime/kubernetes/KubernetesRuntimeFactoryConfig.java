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
package org.apache.pulsar.functions.runtime.kubernetes;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.nar.NarClassLoader;

import java.util.Map;

@Data
@Accessors(chain = true)
public class KubernetesRuntimeFactoryConfig {
    @FieldContext(
        doc = "Uri to kubernetes cluster, leave it to empty and it will use the kubernetes settings in"
            + " function worker machine"
    )
    protected String k8Uri;
    @FieldContext(
        doc = "The Kubernetes namespace to run the function instances. It is `default`,"
            + " if this setting is left to be empty"
    )
    protected String jobNamespace;
    @FieldContext(
            doc = "The Kubernetes pod name to run the function instances. It is set to"
                + "`pf-<tenant>-<namespace>-<function_name>-<random_uuid(8)>` if this setting is left to be empty"
        )
    protected String jobName;
    @FieldContext(
        doc = "The docker image used to run function instance. By default it is `apachepulsar/pulsar`"
    )
    protected String pulsarDockerImageName;

    @FieldContext(
            doc = "The function docker images used to run function instance according to different "
                    + "configurations provided by users. By default it is `apachepulsar/pulsar`"
    )
    protected Map<String, String> functionDockerImages;

    @FieldContext(
            doc = "The image pull policy for image used to run function instance. By default it is `IfNotPresent`"
    )
    protected String imagePullPolicy;
    @FieldContext(
            doc = "The root directory of pulsar home directory in the pulsar docker image specified"
                    + " `pulsarDockerImageName`. By default it is under `/pulsar`. If you are using your own"
                    + " customized image in `pulsarDockerImageName`, you need to set this setting accordingly"
    )
    protected String pulsarRootDir;
    @FieldContext(
            doc = "The config admin CLI allows users to customize the configuration of the admin cli tool, such as:"
                    + " `/bin/pulsar-admin and /bin/pulsarctl`. By default it is `/bin/pulsar-admin`. If you want to use `pulsarctl` "
                    + " you need to set this setting accordingly"
    )
    protected String configAdminCLI;
    @FieldContext(
        doc = "This setting only takes effects if `k8Uri` is set to null. If your function worker is"
            + " also running as a k8s pod, set this to `true` is let function worker to submit functions to"
            + " the same k8s cluster as function worker is running. Set this to `false` if your function worker"
            + " is not running as a k8s pod"
    )
    protected Boolean submittingInsidePod;
    @FieldContext(
        doc = "The pulsar service url that pulsar functions should use to connect to pulsar."
            + " If it is not set, it will use the pulsar service url configured in function worker."
    )
    protected String pulsarServiceUrl;
    @FieldContext(
        doc = "The pulsar admin url that pulsar functions should use to connect to pulsar."
            + " If it is not set, it will use the pulsar admin url configured in function worker."
    )
    protected String pulsarAdminUrl;
    @FieldContext(
        doc = "The flag indicates to install user code dependencies. (applied to python package)"
    )
    protected Boolean installUserCodeDependencies;
    @FieldContext(
        doc = "The repository that pulsar functions use to download python dependencies"
    )
    protected String pythonDependencyRepository;
    @FieldContext(
        doc = "The repository that pulsar functions use to download extra python dependencies"
    )
    protected String pythonExtraDependencyRepository;

    @FieldContext(
        doc = "the directory for dropping extra function dependencies. "
            + "If it is not absolute path, it is relative to `pulsarRootDir`"
    )
    protected String extraFunctionDependenciesDir;
    @FieldContext(
        doc = "The custom labels that function worker uses to select the nodes for pods"
    )
    protected Map<String, String> customLabels;

    @FieldContext(
        doc = "The expected metrics collection interval, in seconds"
    )
    protected Integer expectedMetricsCollectionInterval = 30;
    @FieldContext(
        doc = "Kubernetes Runtime will periodically checkback on"
            + " this configMap if defined and if there are any changes"
            + " to the kubernetes specific stuff, we apply those changes"
    )
    protected String changeConfigMap;
    @FieldContext(
        doc = "The namespace for storing change config map"
    )
    protected String changeConfigMapNamespace;

    @FieldContext(
            doc = "Additional memory padding added on top of the memory requested by the function per on a per instance basis"
    )
    protected int percentMemoryPadding;

    @FieldContext(
            doc = "The ratio cpu request and cpu limit to be set for a function/source/sink." +
                    "  The formula for cpu request is cpuRequest = userRequestCpu / cpuOverCommitRatio"
    )
    protected double cpuOverCommitRatio = 1.0;

    @FieldContext(
            doc = "The ratio memory request and memory limit to be set for a function/source/sink." +
                    "  The formula for memory request is memoryRequest = userRequestMemory / memoryOverCommitRatio"
    )
    protected double memoryOverCommitRatio = 1.0;

    @FieldContext(
      doc = "The port inside the function pod which is used by the worker to communicate with the pod"
    )
    private Integer grpcPort = 9093;

    @FieldContext(
      doc = "The port inside the function pod on which prometheus metrics are exposed"
    )
    private Integer metricsPort = 9094;

    @FieldContext(
       doc = "The directory inside the function pod where nar packages will be extracted"
    )
    private String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;

    @FieldContext(
            doc = "The classpath where function instance files stored"
    )
    private String functionInstanceClassPath = "";
    @FieldContext(
            doc = "The duration in seconds before the StatefulSet deleted on function stop/restart. " +
                    "Value must be non-negative integer. The value zero indicates delete immediately. " +
                    "Default is 5 seconds."
    )
    protected int gracePeriodSeconds = 5;

}
