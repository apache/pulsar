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
package org.apache.pulsar.common.policies.data;

import com.google.common.base.Objects;

public class ClusterData {
    private String serviceUrl;
    private String serviceUrlTls;
    private String brokerServiceUrl;
    private String brokerServiceUrlTls;

    public ClusterData() {
    }

    public ClusterData(String serviceUrl) {
        this(serviceUrl, "");
    }

    public ClusterData(String serviceUrl, String serviceUrlTls) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
    }
    
    public ClusterData(String serviceUrl, String serviceUrlTls, String brokerServiceUrl, String brokerServiceUrlTls) {
        this.serviceUrl = serviceUrl;
        this.serviceUrlTls = serviceUrlTls;
        this.brokerServiceUrl = brokerServiceUrl;
        this.brokerServiceUrlTls = brokerServiceUrlTls;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public String getServiceUrlTls() {
        return serviceUrlTls;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public void setServiceUrlTls(String serviceUrlTls) {
        this.serviceUrlTls = serviceUrlTls;
    }
    
    public String getBrokerServiceUrl() {
        return brokerServiceUrl;
    }

    public void setBrokerServiceUrl(String brokerServiceUrl) {
        this.brokerServiceUrl = brokerServiceUrl;
    }

    public String getBrokerServiceUrlTls() {
        return brokerServiceUrlTls;
    }

    public void setBrokerServiceUrlTls(String brokerServiceUrlTls) {
        this.brokerServiceUrlTls = brokerServiceUrlTls;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ClusterData) {
            ClusterData other = (ClusterData) obj;
            return Objects.equal(serviceUrl, other.serviceUrl) && Objects.equal(serviceUrlTls, other.serviceUrlTls)
                    && Objects.equal(brokerServiceUrl, other.brokerServiceUrl)
                    && Objects.equal(brokerServiceUrlTls, other.brokerServiceUrlTls);
        }

        return false;
    }

    @Override
    public int hashCode() {
       return Objects.hashCode(this.toString());
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append(serviceUrl);
        if (serviceUrlTls != null && !serviceUrlTls.isEmpty()) {
            str.append(",");
            str.append(serviceUrlTls);
        }
        if (brokerServiceUrl != null && !brokerServiceUrl.isEmpty()) {
            str.append(",");
            str.append(brokerServiceUrl);
        }
        if (brokerServiceUrlTls != null && !brokerServiceUrlTls.isEmpty()) {
            str.append(",");
            str.append(brokerServiceUrlTls);
        }
        return str.toString();
    }
    
}
