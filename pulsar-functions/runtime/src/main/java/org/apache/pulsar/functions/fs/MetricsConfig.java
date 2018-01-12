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
package org.apache.pulsar.functions.fs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
/**
 * This represents the config related to the resource limits of function calls
 */
public class MetricsConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String metricsSinkClassName;
    private int metricsCollectionInterval;
    private Map<String, String> metricsSinkConfig;

    public static MetricsConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), MetricsConfig.class);
    }

}
