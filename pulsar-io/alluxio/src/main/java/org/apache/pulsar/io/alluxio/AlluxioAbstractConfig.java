/*
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
package org.apache.pulsar.io.alluxio;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Configuration object for all Alluxio Sink components.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class AlluxioAbstractConfig implements Serializable {

    private static final long serialVersionUID = 3727671407445918309L;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The hostname of Alluxio master")
    private String alluxioMasterHost;

    @FieldDoc(
        required = true,
        defaultValue = "19998",
        help = "The port that Alluxio master node runs on")
    private int alluxioMasterPort = 19998;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The Alluxio directory from which files should be read from or written to")
    private String alluxioDir;

    @FieldDoc(
        required = false,
        defaultValue = "",
        help = "When `alluxio.security.authentication.type` is set to `SIMPLE` or `CUSTOM`, user application uses"
            + " this property to indicate the user requesting Alluxio service. If it is not set explicitly,"
            + " the OS login user is used")
    private String securityLoginUser;

    public void validate() {
        checkNotNull(alluxioMasterHost, "alluxioMasterHost property not set.");
        checkNotNull(alluxioMasterPort, "alluxioMasterPort property not set.");
        checkNotNull(alluxioDir, "alluxioDir property not set.");
    }
}
