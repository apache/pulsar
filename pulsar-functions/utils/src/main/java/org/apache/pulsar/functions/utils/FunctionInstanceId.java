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
package org.apache.pulsar.functions.utils;

import lombok.Data;

@Data
public class FunctionInstanceId {

    private String tenant;
    private String namespace;
    private String name;
    private int instanceId;

    public FunctionInstanceId(String fullyQualifiedInstanceName) {
        String[] t1 = fullyQualifiedInstanceName.split("/");

        if (t1.length != 3) {
            throw new IllegalArgumentException("Invalid format for fully qualified instance name: " + fullyQualifiedInstanceName);
        }

        this.tenant = t1[0];
        this.namespace = t1[1];

        int instanceIdDelimiterIndex = t1[2].lastIndexOf(':');

        if (instanceIdDelimiterIndex < 0) {
            throw new IllegalArgumentException("Invalid format for fully qualified instance name: " + fullyQualifiedInstanceName);            
        }
        
        this.name = t1[2].substring(0, instanceIdDelimiterIndex);
        this.instanceId = Integer.parseInt(t1[2].substring(instanceIdDelimiterIndex + 1));
    }
}
