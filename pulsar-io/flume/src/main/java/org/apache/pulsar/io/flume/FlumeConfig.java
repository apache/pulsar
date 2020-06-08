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
package org.apache.pulsar.io.flume;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 *  Flume general config.
 */
@Data
@Accessors(chain = true)
public class FlumeConfig {

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "the name of this agent")
    private String name;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "specify a config file (required if -z missing)")
    private String confFile;
    @FieldDoc(
            defaultValue = "false",
            help = "do not reload config file if changed")
    private Boolean noReloadConf;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "specify the ZooKeeper connection to use (required if -f missing)")
    private String zkConnString;
    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "specify the base path in ZooKeeper for agent configs")
    private String zkBasePath;

    public static FlumeConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), FlumeConfig.class);
    }


    public static FlumeConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), FlumeConfig.class);
    }
}
