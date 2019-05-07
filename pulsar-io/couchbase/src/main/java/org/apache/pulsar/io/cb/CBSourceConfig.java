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
package org.apache.pulsar.io.cb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@Accessors(chain = true)
public class CBSourceConfig implements Serializable {
  @FieldDoc(required = true, defaultValue = "",
      help = "name connection to couchbase server")
  private String connectionName;

  @FieldDoc(required = true, defaultValue = "10000",
      help = "Wait this milliseconds before failing when connecting to couchbase server")
  private long connectionTimeOut;

  @FieldDoc(required = true, defaultValue = "",
      help = "hostnames of couchbase server")
  private String[] hostnames;

  @FieldDoc(required = true, defaultValue = "",
      help = "bucket to connect on couchbase server")
  private String bucket;

  @FieldDoc(required = true, defaultValue = "", sensitive = true,
      help = "username to connect to bucket on couchbase server")
  private String username;

  @FieldDoc(required = true, defaultValue = "", sensitive = true,
      help = "password to connect to bucket on couchbase server")
  private String password;

  @FieldDoc(required = true, defaultValue = "ENABLED", sensitive = true,
      help = "password to connect to bucket on couchbase server")
  private String compressedMode;

  @FieldDoc(required = true, defaultValue = "",
      help = "persistence polling interval in milliseconds for rollback mitigation")
  private long persistencePollingInterval;

  @FieldDoc(required = true, defaultValue = "",
      help = "the amount of data the server will send before requiring an ack")
  private int flowControlBufferSizeInBytes;

  @FieldDoc(required = true, defaultValue = "false",
      help = "set if ssl should be enabled")
  private Boolean sslEnabled;

  @FieldDoc(required = true, defaultValue = "",
      help = "the location of the ssl keystore file")
  private String sslKeystoreFile;

  @FieldDoc(required = true, defaultValue = "",
      help = "the ssl keystore password to be used with the keystore file")
  private String sslKeystorePassword;

  @FieldDoc(required = true, defaultValue = "false",
      help = "use snapshots")
  private Boolean useSnapshots;

  public static CBSourceConfig load(String yamlFile) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(new File(yamlFile), CBSourceConfig.class);
  }

  public static CBSourceConfig load(Map<String, Object> map) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new ObjectMapper().writeValueAsString(map), CBSourceConfig.class);
  }

}
