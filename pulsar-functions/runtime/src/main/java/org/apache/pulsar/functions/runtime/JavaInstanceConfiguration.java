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
package org.apache.pulsar.functions.runtime;

import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;
import org.apache.pulsar.common.configuration.PulsarConfiguration;
import org.apache.pulsar.common.nar.NarClassLoader;

// TODO do we have to set the default Configuration ?

@Getter
@Setter
@ToString
public class JavaInstanceConfiguration implements PulsarConfiguration {
    @Category
    private static final String CATEGORY_FUNCTIONS = "Functions";

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Function details json"
    )
    private String functionDetailsJsonString;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Path to user function jar"
    )
    private String jarFile;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Path to the transform function jar"
    )
    private String transformFunctionJarFile;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "instanceId used to uniquely identify a function instance"
    )
    private Integer instanceId;


    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "functionId used to uniquely identify a function"
    )
    private String functionId;


    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "The version of the function"
    )
    private String functionVersion;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "serviceUrl of the target Pulsar cluster"
    )
    private String pulsarServiceUrl;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "functionId of the transform function"
    )
    private String transformFunctionId;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Client auth plugin full classname"
    )
    private String clientAuthenticationPlugin;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Client auth parameters"
    )
    private String clientAuthenticationParameters;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Use tls connection"
    )
    private String useTls = Boolean.FALSE.toString();

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Allow insecure tls connection"
    )
    private String tlsAllowInsecureConnection = Boolean.TRUE.toString();

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Enable hostname verification"
    )
    private String tlsHostNameVerificationEnabled = Boolean.FALSE.toString();

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "tls trust cert file path"
    )
    private String tlsTrustCertFilePath;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "State storage service implementation classname"
    )
    private String stateStorageImplClass;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "State storage service url"
    )
    private String stateStorageServiceUrl;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Port to listen on"
    )
    private Integer port;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Port metrics will be exposed on"
    )
    private Integer metricsPort;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Maximum number of tuples to buffer"
    )
    private Integer maxBufferedTuples;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Expected interval in seconds between health checks"
    )
    private Integer expectedHealthCheckInterval;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "The classname of the secrets provider"
    )
    private String secretsProviderClassName;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "The config that needs to be passed to secrets provider"
    )
    private String secretsProviderConfig;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "The name of the cluster this instance is running on"
    )
    private String clusterName;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "The directory where extraction of nar packages happen"
    )
    private String narExtractionDirectory = NarClassLoader.DEFAULT_NAR_EXTRACTION_DIR;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Max pending async requests per instance"
    )
    private Integer maxPendingAsyncRequests = 1000;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Pulsar Web Service Url"
    )
    private String webServiceUrl;

    @FieldContext(
            category = CATEGORY_FUNCTIONS,
            doc = "Whether the pulsar admin client exposed to function context, default is disabled"
    )
    private Boolean exposePulsarAdminClientEnabled = false;


    @ToString.Exclude
    @com.fasterxml.jackson.annotation.JsonIgnore
    private Properties properties = new Properties();

    public Object getProperty(String key) {
        return properties.get(key);
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
