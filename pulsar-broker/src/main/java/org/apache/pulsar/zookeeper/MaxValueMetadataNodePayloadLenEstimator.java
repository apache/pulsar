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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.StringSplitter;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataNodePayloadLenEstimator;

@Slf4j
public class MaxValueMetadataNodePayloadLenEstimator implements MetadataNodePayloadLenEstimator {

    // Default to max int value, let the first query command do not execute with batch.
    public static final int DEFAULT_LEN = Integer.MAX_VALUE;
    public static final int UNSET = -1;
    public static final int ZK_PACKET_HEADER_LEN = 160;
    private static final SplitPathRes MEANINGLESS_SPLIT_PATH_RES = new SplitPathRes();
    private final int[] maxLenOfGetMapping;
    private final int[] maxLenOfListMapping;

    {
        int PathTypeCount = PathType.values().length;
        maxLenOfGetMapping = new int[PathTypeCount];
        maxLenOfListMapping = new int[PathTypeCount];
        for (int i = 0; i < PathTypeCount; i++) {
            maxLenOfGetMapping[i] = UNSET;
            maxLenOfListMapping[i] = UNSET;
        }
    }

    @Override
    public void recordPut(String path, byte[] data) {
        PathType pathType = getPathType(path);
        maxLenOfGetMapping[pathType.ordinal()] = Math.max(maxLenOfGetMapping[pathType.ordinal()], data.length);
    }

    @Override
    public void recordGetRes(String path, GetResult getResult) {
        PathType pathType = getPathType(path);
        if (getResult == null) {
            return;
        }
        maxLenOfGetMapping[pathType.ordinal()] = Math.max(maxLenOfGetMapping[pathType.ordinal()],
                getResult.getValue().length);
    }

    @Override
    public void recordGetChildrenRes(String path, List<String> list) {
        PathType pathType = getPathType(path);
        int totalLen = list.stream().map(String::length).reduce(0, Integer::sum);
        maxLenOfListMapping[pathType.ordinal()] = Math.max(maxLenOfListMapping[pathType.ordinal()], totalLen);
    }

    @Override
    public int estimateGetResPayloadLen(String path) {
        PathType pathType = getPathType(path);
        int res = maxLenOfGetMapping[pathType.ordinal()];
        return res == UNSET ? DEFAULT_LEN : res + ZK_PACKET_HEADER_LEN;
    }

    @Override
    public int estimateGetChildrenResPayloadLen(String path) {
        PathType pathType = getPathType(path);
        int res = maxLenOfListMapping[pathType.ordinal()];
        return res == UNSET ? DEFAULT_LEN : res + ZK_PACKET_HEADER_LEN;
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

    static class SplitPathRes {
        String[] parts = new String[2];
        int partCount;
    }

    /**
     * Split the path by the delimiter '/', calculate pieces count and only keep the first two parts.
     */
    static SplitPathRes splitPath(String path) {
        if (path == null || path.length() <= 1) {
            return MEANINGLESS_SPLIT_PATH_RES;
        }
        SplitPathRes res = new SplitPathRes();
        String[] parts = res.parts;
        char delimiter = '/';
        int length = path.length();
        int start = 0;
        int count = 0;
        for (int i = 0; i < length; i++) {
            if (path.charAt(i) == delimiter) {
                // Skip the first and the latest delimiter.
                if (start == i) {
                    start = i + 1;
                    continue;
                }
                // Only keep the first two parts.
                if (count < 2) {
                    parts[count] = path.substring(start, i);
                }
                start = i + 1;
                count++;
            }
        }
        if (start < length - 1) {
            count++;
        }
        res.partCount = count;
        return res;
    }
}
