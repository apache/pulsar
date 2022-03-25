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
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import org.apache.pulsar.io.hbase.HbaseAbstractConfig;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
public class HbaseSinkConfig extends HbaseAbstractConfig {

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

    @FieldDoc(
       defaultValue = "1000l",
       help = "The hbase operation time in milliseconds")
    private long batchTimeMs = 1000L;

    @FieldDoc(
        defaultValue = "200",
        help = "The batch size of write to the hbase table"
    )
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
        Preconditions.checkNotNull(rowKeyName, "rowKeyName property not set.");
        Preconditions.checkNotNull(familyName, "familyName property not set.");
        Preconditions.checkNotNull(qualifierNames, "qualifierNames property not set.");
        Preconditions.checkArgument(batchTimeMs > 0, "batchTimeMs must be a positive long.");
        Preconditions.checkArgument(batchSize > 0, "batchSize must be a positive integer.");
    }
}
