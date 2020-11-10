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

import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.RuntimeCustomizer;

public interface KubernetesManifestCustomizer extends RuntimeCustomizer {

    default V1StatefulSet customizeStatefulSet(Function.FunctionDetails funcDetails, V1StatefulSet statefulSet) {
        return statefulSet;
    }

    default V1Service customizeService(Function.FunctionDetails funcDetails, V1Service service) {
        return service;
    }

    default String customizeNamespace(Function.FunctionDetails funcDetails, String currentNamespace) {
        return currentNamespace;
    }
    
    default String customizeName(Function.FunctionDetails funcDetails, String currentName) {
        return currentName;
    }
}
