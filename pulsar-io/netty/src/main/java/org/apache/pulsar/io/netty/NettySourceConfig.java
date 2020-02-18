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
package org.apache.pulsar.io.netty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Netty Source Connector Config.
 */
@Data
@Accessors(chain = true)
public class NettySourceConfig implements Serializable {

    private static final long serialVersionUID = -7116130435021510496L;

    @FieldDoc(
            required = true,
            defaultValue = "tcp",
            help = "The network protocol to use, supported values are 'tcp', 'udp', and 'http'")
    private String type = "tcp";

    @FieldDoc(
            required = true,
            defaultValue = "127.0.0.1",
            help = "The host name or address that the source instance to listen on")
    private String host = "127.0.0.1";

    @FieldDoc(
            required = true,
            defaultValue = "10999",
            help = "The port that the source instance to listen on")
    private int port = 10999;

    @FieldDoc(
            required = true,
            defaultValue = "1",
            help = "The number of threads of Netty Tcp Server to accept incoming connections and "
                    + "handle the traffic of the accepted connections")
    private int numberOfThreads = 1;

    public static NettySourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), NettySourceConfig.class);
    }

    public static NettySourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), NettySourceConfig.class);
    }

}
