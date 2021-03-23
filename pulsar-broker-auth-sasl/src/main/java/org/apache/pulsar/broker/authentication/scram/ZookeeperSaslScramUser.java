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
package org.apache.pulsar.broker.authentication.scram;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ZookeeperSaslScramUser {

    private String password = null;

    private String scramSha256 = null;

    public ZookeeperSaslScramUser() {
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getScramSha256() {
        return scramSha256;
    }

    @JsonProperty("SCRAM_SHA_256")
    public void setScramSha256(String scramSha256) {
        this.scramSha256 = scramSha256;
    }

}
