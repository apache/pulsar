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

package org.apache.pulsar.functions.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;

public class FunctionDetailsUtils {

    public static String getFullyQualifiedName(FunctionDetails FunctionDetails) {
        return getFullyQualifiedName(FunctionDetails.getTenant(), FunctionDetails.getNamespace(), FunctionDetails.getName());
    }

    public static String getFullyQualifiedName(String tenant, String namespace, String functionName) {
        return String.format("%s/%s/%s", tenant, namespace, functionName);
    }

    public static String extractTenantFromFQN(String fullyQualifiedName) {
        return fullyQualifiedName.split("/")[0];
    }

    public static String extractNamespaceFromFQN(String fullyQualifiedName) {
        return fullyQualifiedName.split("/")[1];
    }

    public static String extractFunctionNameFromFQN(String fullyQualifiedName) {
        return fullyQualifiedName.split("/")[2];
    }

    public static String getDownloadFileName(FunctionDetails FunctionDetails,
                                             Function.PackageLocationMetaData packageLocation) {
        if (!StringUtils.isEmpty(packageLocation.getOriginalFileName())) {
            return packageLocation.getOriginalFileName();
        }
        String[] hierarchy = FunctionDetails.getClassName().split("\\.");
        String fileName;
        if (hierarchy.length <= 0) {
            fileName = FunctionDetails.getClassName();
        } else if (hierarchy.length == 1) {
            fileName =  hierarchy[0];
        } else {
            fileName = hierarchy[hierarchy.length - 2];
        }
        switch (FunctionDetails.getRuntime()) {
            case JAVA:
                return fileName + ".jar";
            case PYTHON:
                return fileName + ".py";
            default:
                throw new RuntimeException("Unknown runtime " + FunctionDetails.getRuntime());
        }
    }
}
