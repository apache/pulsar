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

package org.apache.pulsar.io.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Accessors(chain = true)
public class JdbcSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "Username used to connect to the database specified by `jdbcUrl`"
    )
    private String userName;
    @FieldDoc(
        required = false,
        defaultValue = "",
        sensitive = true,
        help = "Password used to connect to the database specified by `jdbcUrl`"
    )
    private String password;
    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The JDBC url of the database this connector connects to"
    )
    private String jdbcUrl;
    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The name of the table this connector writes messages to"
    )
    private String tableName;
    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Fields used in update events. A comma-separated list."
    )
    private String nonKey;
    @FieldDoc(
            required = false,
            defaultValue = "",
            help = "Fields used in where condition of update and delete Events. A comma-separated list."
    )
    private String key;
    @FieldDoc(
        required = false,
        defaultValue = "500",
        help = "The jdbc operation timeout in milliseconds"
    )
    private int timeoutMs = 500;
    @FieldDoc(
        required = false,
        defaultValue = "200",
        help = "The batch size of updates made to the database"
    )
    private int batchSize = 200;

    public static JdbcSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), JdbcSinkConfig.class);
    }

    public static JdbcSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), JdbcSinkConfig.class);
    }
}
