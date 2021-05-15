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
package org.apache.pulsar.io.dis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * @author shoothzj
 */
@Data
@Accessors(chain = true)
public class DisConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(required = true, defaultValue = "", help = "Information about the region where the DIS is located")
    private String region;

    @FieldDoc(required = true, defaultValue = "", help = "Domain name or IP address of the server bearing the DIS REST service.")
    private String disEndpoint;

    @FieldDoc(required = true, defaultValue = "", help = "User Project ID")
    private String projectId;

    @FieldDoc(required = true, defaultValue = "", help = "User AK")
    private String ak;

    @FieldDoc(required = true, defaultValue = "", help = "User SK")
    private String sk;

    @FieldDoc(required = true, defaultValue = "", help = "Id of the DIS stream")
    private String streamId;

    @FieldDoc(required = true, defaultValue = "", help = "Name of the DIS stream")
    private String streamName;

    public static DisConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), DisConfig.class);
    }

    public static DisConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), DisConfig.class);
    }

}
