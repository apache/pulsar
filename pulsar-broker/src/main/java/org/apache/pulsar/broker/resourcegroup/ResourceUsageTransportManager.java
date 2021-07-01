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
package org.apache.pulsar.broker.resourcegroup;

public interface ResourceUsageTransportManager extends AutoCloseable {

  /*
   * Register a resource owner (resource-group, tenant, namespace, topic etc).
   *
   * @param resource usage publisher
   */
  void registerResourceUsagePublisher(ResourceUsagePublisher r);

  /*
   * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
   *
   * @param resource usage publisher
   */
  void unregisterResourceUsagePublisher(ResourceUsagePublisher r);

  /*
   * Register a resource owner (resource-group, tenant, namespace, topic etc).
   *
   * @param resource usage consumer
   */
  void registerResourceUsageConsumer(ResourceUsageConsumer r);

  /*
   * Unregister a resource owner (resource-group, tenant, namespace, topic etc).
   *
   * @param resource usage consumer
   */
  void unregisterResourceUsageConsumer(ResourceUsageConsumer r);

  ResourceUsageTransportManager DISABLE_RESOURCE_USAGE_TRANSPORT_MANAGER = new ResourceUsageDisabledTransportManager();

  class ResourceUsageDisabledTransportManager implements ResourceUsageTransportManager {
    @Override
    public void registerResourceUsagePublisher(ResourceUsagePublisher r) {
    }

    @Override
    public void unregisterResourceUsagePublisher(ResourceUsagePublisher r) {
    }

    @Override
    public void registerResourceUsageConsumer(ResourceUsageConsumer r) {
    }

    public void unregisterResourceUsageConsumer(ResourceUsageConsumer r) {
    }

    @Override
    public void close() throws Exception {

    }
  }

}
