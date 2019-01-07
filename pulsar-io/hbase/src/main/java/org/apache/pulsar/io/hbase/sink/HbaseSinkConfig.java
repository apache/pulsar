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
package org.apache.pulsar.io.hbase.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.apache.pulsar.io.hbase.HbaseAbstractConfig;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
@Setter
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
@Accessors(chain = true)
public class HbaseSinkConfig extends HbaseAbstractConfig implements Serializable {

    private static final long serialVersionUID = 1245636479605735555L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The hbase table rowkey name")
    private String rowKeyName;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The hbase table column family name")
    private String familyName;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The hbase table column qualifier names")
    private List<String> qualifierNames;


    /**
     * The hbase operation timeout in milliseconds
     */
    @FieldDoc(
        required = false,
        defaultValue = "3000",
        help = "The hbase table operation timeout in milliseconds")
    private int timeoutMs = 3000;

    @FieldDoc(
        required = false,
        defaultValue = "200",
        help = "The batch size of updates made to the hbase table")
    private int batchSize = 200;

    public static HbaseSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), HbaseSinkConfig.class);
    }

    public static HbaseSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), HbaseSinkConfig.class);
    }

    @Override
    public void validate() {
        super.validate();

        if (StringUtils.isEmpty(rowKeyName) ||
                StringUtils.isEmpty(familyName) ||
                CollectionUtils.isEmpty(qualifierNames)) {
            throw new IllegalArgumentException("Required property not set.");
        }

        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeout cannot be negative");
        }

        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be a positive integer");
        }
    }
}
