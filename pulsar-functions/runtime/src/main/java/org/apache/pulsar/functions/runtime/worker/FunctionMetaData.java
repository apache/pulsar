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
package org.apache.pulsar.functions.runtime.worker;

import com.google.gson.Gson;

import java.io.Serializable;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.apache.pulsar.functions.runtime.spawner.Spawner;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class FunctionMetaData implements Serializable, Cloneable {

    // function config
    private FunctionConfig functionConfig;
    // resource / limits config
    private LimitsConfig limitsConfig;
    // function package location
    private PackageLocationMetaData packageLocation;
    private String runtime;
    // the version of this function state
    private long version;
    // the timestamp when the function was created
    private long createTime;
    private String workerId;
    // Did we encounter any exception starting this function
    private Exception startupException;

    public void incrementVersion() {
        this.version = this.version + 1;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public FunctionMetaData fromJson(String json) {
        return new Gson().fromJson(json, this.getClass());
    }

    @Override
    public Object clone() {
        return this.fromJson(this.toJson());
    }

}
