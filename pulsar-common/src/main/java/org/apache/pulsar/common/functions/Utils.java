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
package org.apache.pulsar.common.functions;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import java.util.Arrays;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.packages.management.core.common.PackageType;

/**
 * Helper class to work with configuration.
 */
public class Utils {
    public static final String HTTP = "http";
    public static final String FILE = "file";
    public static final String BUILTIN = "builtin";

    public static boolean isFunctionPackageUrlSupported(String functionPkgUrl) {
        return isNotBlank(functionPkgUrl) && (functionPkgUrl.startsWith(HTTP)
                || functionPkgUrl.startsWith(FILE)
                || hasPackageTypePrefix(functionPkgUrl));
    }

    public static boolean hasPackageTypePrefix(String destPkgUrl) {
        return Arrays.stream(PackageType.values()).anyMatch(type -> destPkgUrl.startsWith(type.toString())
                && destPkgUrl.contains("://"));
    }

    public static void inferMissingFunctionName(FunctionConfig functionConfig) {
        String[] domains = functionConfig.getClassName().split("\\.");
        if (domains.length == 0) {
            functionConfig.setName(functionConfig.getClassName());
        } else {
            functionConfig.setName(domains[domains.length - 1]);
        }
    }

    public static void inferMissingTenant(FunctionConfig functionConfig) {
        functionConfig.setTenant(PUBLIC_TENANT);
    }

    public static void inferMissingNamespace(FunctionConfig functionConfig) {
        functionConfig.setNamespace(DEFAULT_NAMESPACE);
    }

    public static void inferMissingArguments(SourceConfig sourceConfig) {
        if (sourceConfig.getTenant() == null) {
            sourceConfig.setTenant(PUBLIC_TENANT);
        }
        if (sourceConfig.getNamespace() == null) {
            sourceConfig.setNamespace(DEFAULT_NAMESPACE);
        }
        if (sourceConfig.getParallelism() == null) {
            sourceConfig.setParallelism(1);
        }
    }

    public static void inferMissingArguments(SinkConfig sinkConfig) {
        if (sinkConfig.getTenant() == null) {
            sinkConfig.setTenant(PUBLIC_TENANT);
        }
        if (sinkConfig.getNamespace() == null) {
            sinkConfig.setNamespace(DEFAULT_NAMESPACE);
        }
        if (sinkConfig.getParallelism() == null) {
            sinkConfig.setParallelism(1);
        }
    }
}
