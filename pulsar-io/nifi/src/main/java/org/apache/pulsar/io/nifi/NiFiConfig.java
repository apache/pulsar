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
package org.apache.pulsar.io.nifi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Configuration object for all nifi components.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class NiFiConfig implements Serializable {

    private static final long serialVersionUID = 4013162911059315334L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Specifies the URL of the remote NiFi instance.")
    private String url;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "Specifies the name of the port to communicate with. Either the port name or the port identifier must be specified.")
    private String portName;

    @FieldDoc(
            required = false,
            defaultValue = "5",
            help = "the client has the ability to request particular batch size/duration.")
    private int requestBatchCount = 5;

    @FieldDoc(
            required = false,
            defaultValue = "1000",
            help = "the amount of time to wait (in milliseconds) if no data is available to pull from NiFi.")
    private long waitTimeMs = 1000l;

    public static NiFiConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), NiFiConfig.class);
    }

    public static NiFiConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), NiFiConfig.class);
    }

    public void validate() {
        if (StringUtils.isEmpty(url)  ||
                StringUtils.isEmpty(portName)) {
            throw new IllegalArgumentException("Required property not set.");
        }
    }
}
