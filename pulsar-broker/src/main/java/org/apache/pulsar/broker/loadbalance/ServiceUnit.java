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
package org.apache.pulsar.broker.loadbalance;

/*
    ServiceUnit is a smallest indivisible set that any broker can pick up and service. This unit will be moved around
    and also turned on and off as and when needed. Load Manager will decide the placement of these Units to minimize
    the load on ResourceUnit and maximize the performance for each ServiceUnit.
 */
public abstract class ServiceUnit implements Comparable<ServiceUnit> {
    public abstract String getServiceUnitId();

    public abstract ResourceDescription getResourceDescription();

    public abstract ServiceRequest getServiceRequest();

    public abstract void setResourceDescription(ResourceDescription serviceUnitResourceUsage);
}
