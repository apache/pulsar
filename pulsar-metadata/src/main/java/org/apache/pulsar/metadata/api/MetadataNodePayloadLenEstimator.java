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

public interface MetadataNodePayloadLenEstimator {

    /**
     * Record the payload length of put operation, to let the estimator know the max payload length, which benefits the
     * accuracy of estimation.
     */
    void recordPut(String path, byte[] data);

    /**
     * Record the payload length of get result, to let the estimator know the max payload length, which benefits the
     * accuracy of estimation.
     */
    void recordGetRes(String path, GetResult getResult);

    /**
     * Record the payload length of list result, to let the estimator know the max payload length, which benefits the
     * accuracy of estimation.
     */
    void recordGetChildrenRes(String path, List<String> list);

    /**
     * Estimate the payload length of get result.
     */
    int estimateGetResPayloadLen(String path);

    /**
     * Estimate the payload length of list result.
     */
    int estimateGetChildrenResPayloadLen(String path);
}