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
package org.apache.pulsar.zookeeper;

import java.util.HashMap;
import java.util.List;
import org.apache.pulsar.common.util.StringSplitter;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataNodePayloadLenEstimator;

public class MaxValueMetadataNodePayloadLenEstimator implements MetadataNodePayloadLenEstimator {

    private final HashMap<PathType, Integer> maxLenPerPathType = new HashMap<>();
    private final HashMap<PathType, Integer> maxLenPerListPathType = new HashMap<>();
    // Default to max int value, let the first query command do not execute with batch.
    private static final int DEFAULT_LEN = 1;//Integer.MAX_VALUE;

    @Override
    public void recordPut(String path, byte[] data) {
        PathType pathType = getPathType(path);
        maxLenPerPathType.compute(pathType, (k, v) -> {
            if (v == null) {
                return data.length;
            }
            return Math.max(v, data.length);
        });
    }

    @Override
    public void recordGetRes(String path, GetResult getResult) {
        PathType pathType = getPathType(path);
        maxLenPerPathType.compute(pathType, (k, v) -> {
            if (v == null) {
                return getResult.getValue().length;
            }
            return Math.max(v, getResult.getValue().length);
        });
    }

    @Override
    public void recordGetChildrenRes(String path, List<String> list) {
        PathType pathType = getPathType(path);
        maxLenPerListPathType.compute(pathType, (k, v) -> {
            if (v == null) {
                return list.stream().map(String::length).reduce(0, Integer::sum);
            }
            return Math.max(v, list.stream().map(String::length).reduce(0, Integer::sum));
        });
    }

    @Override
    public int estimateGetResPayloadLen(String path) {
        PathType pathType = getPathType(path);
        return maxLenPerPathType.getOrDefault(pathType, DEFAULT_LEN);
    }

    @Override
    public int estimateGetChildrenResPayloadLen(String path) {
        PathType pathType = getPathType(path);
        int maxValueCached = maxLenPerListPathType.getOrDefault(pathType, DEFAULT_LEN);
        return maxValueCached + (maxValueCached >> 3);
    }

    private PathType getPathType(String path) {
        String[] parts = StringSplitter.splitByChar(path, '/', 6).toArray(new String[0]);
        if (parts.length < 2) {
            return PathType.OTHERS;
        }
        return switch (parts[0]) {
            case "admin" -> getAdminPathType(parts);
            case "managed-ledgers" -> getMlPathType(parts);
            default -> PathType.OTHERS;
        };
    }

    private PathType getAdminPathType(String[] parts) {
        return switch (parts[1]) {
            case "clusters" -> PathType.CLUSTER;
            case "policies" -> switch (parts.length) {
                case 2 -> PathType.TENANT;
                case 3 -> PathType.NAMESPACE_POLICIES;
                default -> PathType.OTHERS;
            };
            case "partitioned-topics" -> switch (parts.length) {
                case 5 -> PathType.PARTITIONED_NAMESPACE;
                case 6 -> PathType.PARTITIONED_TOPIC;
                default -> PathType.OTHERS;
            };
            default -> PathType.OTHERS;
        };
    }

    private PathType getMlPathType(String[] parts) {
        return switch (parts.length) {
            case 4 -> PathType.ML_NAMESPACE;
            case 5 -> PathType.TOPIC;
            case 6 -> PathType.SUBSCRIPTION;
            default -> PathType.OTHERS;
        };
    }

    enum PathType {
        // admin
        CLUSTER,
        TENANT,
        NAMESPACE_POLICIES,
        // partitioned topics.
        PARTITIONED_TOPIC,
        PARTITIONED_NAMESPACE,
        // managed ledger.
        ML_NAMESPACE,
        TOPIC,
        SUBSCRIPTION,
        OTHERS;
    }
}
