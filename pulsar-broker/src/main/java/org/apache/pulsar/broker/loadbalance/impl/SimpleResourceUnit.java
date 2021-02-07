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
package org.apache.pulsar.broker.loadbalance.impl;

import com.google.common.base.MoreObjects;
import org.apache.pulsar.broker.loadbalance.ResourceDescription;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;

public class SimpleResourceUnit implements ResourceUnit {

    private String resourceId;
    private ResourceDescription resourceDescription;

    public SimpleResourceUnit(String resourceId, ResourceDescription resourceDescription) {
        this.resourceId = resourceId;
        this.resourceDescription = resourceDescription;
    }

    @Override
    public String getResourceId() {
        // TODO Auto-generated method stub
        return resourceId;
    }

    @Override
    public ResourceDescription getAvailableResource() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean canFit(ResourceDescription resourceDescription) {
        // TODO Auto-generated method stub
        return this.resourceDescription.compareTo(resourceDescription) > 0;
    }

    @Override
    public int compareTo(ResourceUnit o) {
        return resourceId.compareTo(o.getResourceId());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SimpleResourceUnit)) {
            return false;
        }
        SimpleResourceUnit other = (SimpleResourceUnit) o;
        return this.resourceId.equals(other.resourceId);
    }

    @Override
    public int hashCode() {
        return this.resourceId.hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("resourceId", resourceId).toString();
    }
}
