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
package org.apache.pulsar.io.datagenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import org.apache.pulsar.config.validation.ConfigValidationAnnotations.PositiveNumber;
import org.apache.pulsar.io.core.annotations.FieldDoc;


@Data
public class DataGeneratorSourceConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  @FieldDoc(
    required = true,
    defaultValue = "50",
    sensitive = false,
    help = "How long to sleep between emitting messages"
  )
  @PositiveNumber
  private long sleepBetweenMessages = 50;


  public static DataGeneratorSourceConfig loadOrGetDefault(Map<String, Object> configMap) {
    if (configMap.isEmpty()) {
      return new DataGeneratorSourceConfig();
    } else {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.convertValue(configMap, DataGeneratorSourceConfig.class);
    }
  }
}
