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

// This is an interim file; it will be replaced by a class that the
// config-manager will provide.

package org.apache.pulsar.broker.resourcegroup;

import io.swagger.annotations.ApiModel;
import lombok.Data;

@Data
@ApiModel(value = "ResourceGroupConfigInfo", description = "Information needed to create or update a resource group.")
public class ResourceGroupConfigInfo {
    private String name;
    private long publishBytesPerPeriod;
    private long publishMessagesPerPeriod;
    private long dispatchBytesPerPeriod;
    private long dispatchMessagesPerPeriod;
}
