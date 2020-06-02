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
package org.apache.pulsar.io.canal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.apache.pulsar.io.core.annotations.FieldDoc;


/**
 * Canal source config.
 */
@Data
@Accessors(chain = true)
public class CanalSourceConfig implements Serializable{

    @FieldDoc(
        required = true,
        defaultValue = "",
        sensitive = true,
        help = "Username to connect to mysql database")
    private String username;
    @FieldDoc(
        required = true,
        defaultValue = "",
        sensitive = true,
        help = "Password to connect to mysql database")
    private String password;
    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "Source destination that Canal source connector connects to")
    private String destination;
    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The mysql database hostname")
    private String singleHostname;
    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "The mysql database port")
    private int singlePort;
    @FieldDoc(
        required = true,
        defaultValue = "false",
        help = "If setting to true, it will be talking to `zkServers` to figure out the actual database hosts."
            + " If setting to false, it will connect to the database specified by `singleHostname` and `singlePort`.")
    private Boolean cluster = false;
    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The zookeeper servers that canal source connector talks to figure out the actual database hosts")
    private String zkServers;
    @FieldDoc(
        required = false,
        defaultValue = "1000",
        help = "The batch size to fetch from canal.")
    private int batchSize = 1000;

    public static CanalSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), CanalSourceConfig.class);
    }


    public static CanalSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), CanalSourceConfig.class);
    }
}
