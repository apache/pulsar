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
package org.apache.pulsar.common.io;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Pulsar Batch Source configuration.
 */
@Getter
@Setter
@Data
@EqualsAndHashCode
@ToString
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class BatchSourceConfig {
    public static final String BATCHSOURCE_CONFIG_KEY = "__BATCHSOURCECONFIGS__";
    public static final String BATCHSOURCE_CLASSNAME_KEY = "__BATCHSOURCECLASSNAME__";

    // The class used for triggering the discovery process
    private String discoveryTriggererClassName;
    // The config needed for the discovery Triggerer init
    private Map<String, Object> discoveryTriggererConfig;
}
