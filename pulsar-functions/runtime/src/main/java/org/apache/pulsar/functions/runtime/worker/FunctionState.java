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

public class FunctionState implements Serializable, Cloneable{

    private String namespace;
    private String tenant;
    private String name;
    private String packageLocation;
    private long maxMemoryMb;
    private long maxCpuCores;
    private long maxTimeMs;
    private String runtime;
    private long version;
    private long createTime;
    private String workerId;
    private String sourceTopic;
    private String sinkTopic;
    private String inputSerdeClassName;
    private String outputSerdeClassName;
    private String className;

    public String getInputSerdeClassName() {
        return inputSerdeClassName;
    }

    public void setInputSerdeClassName(String inputSerdeClassName) {
        this.inputSerdeClassName = inputSerdeClassName;
    }

    public String getOutputSerdeClassName() {
        return outputSerdeClassName;
    }

    public void setOutputSerdeClassName(String outputSerdeClassName) {
        this.outputSerdeClassName = outputSerdeClassName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getSourceTopic() {
        return sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    public String getSinkTopic() {
        return sinkTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPackageLocation() {
        return packageLocation;
    }

    public void setPackageLocation(String packageLocation) {
        this.packageLocation = packageLocation;
    }

    public long getMaxMemoryMb() {
        return maxMemoryMb;
    }

    public void setMaxMemoryMb(long maxMemoryMb) {
        this.maxMemoryMb = maxMemoryMb;
    }

    public long getMaxCpuCores() {
        return maxCpuCores;
    }

    public void setMaxCpuCores(long maxCpuCores) {
        this.maxCpuCores = maxCpuCores;
    }

    public long getMaxTimeMs() {
        return maxTimeMs;
    }

    public void setMaxTimeMs(long maxTimeMs) {
        this.maxTimeMs = maxTimeMs;
    }

    public String getRuntime() {
        return runtime;
    }

    public void setRuntime(String runtime) {
        this.runtime = runtime;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void incrementVersion() {
        this.version = this.version + 1;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public FunctionState fromJson(String json) {
        return new Gson().fromJson(json, this.getClass());
    }

    @Override
    public Object clone() {
        return this.fromJson(this.toJson());
    }

    @Override
    public String toString() {
        return "FunctionState{" +
                "namespace='" + namespace + '\'' +
                ", tenant='" + tenant + '\'' +
                ", name='" + name + '\'' +
                ", packageLocation='" + packageLocation + '\'' +
                ", maxMemoryMb=" + maxMemoryMb +
                ", maxCpuCores=" + maxCpuCores +
                ", maxTimeMs=" + maxTimeMs +
                ", runtime='" + runtime + '\'' +
                ", version=" + version +
                ", createTime=" + createTime +
                ", workerId='" + workerId + '\'' +
                ", sourceTopic='" + sourceTopic + '\'' +
                ", sinkTopic='" + sinkTopic + '\'' +
                ", inputSerdeClassName='" + inputSerdeClassName + '\'' +
                ", outputSerdeClassName='" + outputSerdeClassName + '\'' +
                ", className='" + className + '\'' +
                '}';
    }
}
