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
package org.apache.pulsar.functions.worker.rest;

import org.apache.pulsar.functions.worker.rest.api.FunctionsMetricsResource;
import org.apache.pulsar.functions.worker.rest.api.v2.FunctionApiV2Resource;
import org.apache.pulsar.functions.worker.rest.api.v2.WorkerStats;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Resources {

    private Resources() {
    }

    public static Set<Class<?>> getApiResources() {
        return new HashSet<>(
                Arrays.asList(
                        FunctionApiV2Resource.class,
                        WorkerStats.class,
                        MultiPartFeature.class
                ));
    }

    public static Set<Class<?>> getRootResources() {
        return new HashSet<>(
                Arrays.asList(
                        ConfigurationResource.class,
                        FunctionsMetricsResource.class
                ));
    }
}