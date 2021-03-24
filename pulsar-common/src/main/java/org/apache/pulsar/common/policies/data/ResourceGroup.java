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

import com.google.common.base.MoreObjects;

import java.util.Objects;

public class ResourceGroup {
    public int publishRateInMsgs = -1;
    public long publishRateInBytes = -1;
    public int dispatchRateInMsgs = -1;
    public long dispatchRateInBytes = -1;


    public ResourceGroup() {
        super();
        this.publishRateInMsgs = -1;
        this.publishRateInBytes = -1;
        this.dispatchRateInMsgs = -1;
        this.dispatchRateInBytes = -1;
    }

    public ResourceGroup(int publishRateInMsgs, long publishRateInBytes,
                         int dispatchRateInMsgs, long dispatchRateInBytes) {
        this.publishRateInMsgs = publishRateInMsgs;
        this.publishRateInBytes = publishRateInBytes;
        this.dispatchRateInMsgs = dispatchRateInMsgs;
        this.dispatchRateInBytes = dispatchRateInBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(publishRateInMsgs, publishRateInBytes, dispatchRateInMsgs, dispatchRateInBytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ResourceGroup) {
            ResourceGroup rg = (ResourceGroup) obj;
            return Objects.equals(publishRateInMsgs, rg.publishRateInMsgs)
                    && Objects.equals(publishRateInBytes, rg.publishRateInBytes)
                    && Objects.equals(dispatchRateInMsgs, rg.dispatchRateInMsgs)
                    && Objects.equals(dispatchRateInBytes, rg.dispatchRateInBytes);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("publishRateInMsgs", publishRateInMsgs)
                .add("publishRateInBytes", publishRateInBytes)
                .add("dispatchRateInMsgs", dispatchRateInMsgs)
                .add("dispatchRateInBytes", dispatchRateInBytes).toString();
    }

}
