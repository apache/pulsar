/*
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
package org.apache.pulsar.metadata.api;

import java.util.List;

/***
 * The interface to cache the max payload length of metadata node. It is helpful for the following cases:
 * 1. the limitation of the response of batching query from metadata store. For example, the ZK client limits the max
 *    length of response packet to 1MB by default, if the response packet is larger than the limitation, the ZK client
 *    will throw an error "Packet len {len} is out of range!" and reconnects.
 * 2. expose the metrics of payload length of metadata node.
 */
public interface MetadataNodeSizeStats {

    /**
     * Record the payload length of put operation.
     */
    void recordPut(String path, byte[] data);

    /**
     * Record the payload length of get result.
     */
    void recordGetRes(String path, GetResult getResult);

    /**
     * Record the payload length of list result.
     */
    void recordGetChildrenRes(String path, List<String> list);

    /**
     * Get the max size of same resource type.
     */
    int getMaxSizeOfSameResourceType(String path);

    /**
     * Get the max children count of same resource type.
     */
    int getMaxChildrenCountOfSameResourceType(String path);
}