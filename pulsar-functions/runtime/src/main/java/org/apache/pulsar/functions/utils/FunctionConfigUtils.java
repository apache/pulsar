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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.protobuf.util.JsonFormat;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;

import java.io.File;
import java.io.IOException;

public class FunctionConfigUtils {

    public static String convertYamlToJson(File file) throws IOException {

        ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
        Object obj = yamlReader.readValue(file, Object.class);

        ObjectMapper jsonWriter = new ObjectMapper();
        return jsonWriter.writeValueAsString(obj);
    }

    public static FunctionConfig.Builder loadConfig(File file) throws IOException {
        String json = convertYamlToJson(file);
        FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
        JsonFormat.parser().merge(json, functionConfigBuilder);
        return functionConfigBuilder;
    }

    public static String getFullyQualifiedName(FunctionConfig functionConfig) {
        return String.format("%s/%s/%s", functionConfig.getTenant(), functionConfig.getNamespace(), functionConfig.getName());
    }
}
