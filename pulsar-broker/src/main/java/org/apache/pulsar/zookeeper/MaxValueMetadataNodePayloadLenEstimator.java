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

import io.netty.util.concurrent.FastThreadLocal;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataNodePayloadLenEstimator;

@Slf4j
public class MaxValueMetadataNodePayloadLenEstimator implements MetadataNodePayloadLenEstimator {

    // Default to max int value, let the first query command do not execute with batch.
    public static final int DEFAULT_LEN = Integer.MAX_VALUE;
    public static final int UNSET = -1;
    // xid: int, zxid: long, err: int, len: int -> 20
    // Node stat:
    //   czxid: long, mzxid: long, ctime: long, mtime: long, version: int, cversion: int, aversion: int, -> 44
    //   ephemeralOwner: long, dataLength: int, numChildren: int, pzxid: long -> 24
    // total: 88, let's double it to cover different serialization versions.
    public static final int ZK_PACKET_SYSTEM_PROPS_LEN = 176;
    private static final SplitPathRes MEANINGLESS_SPLIT_PATH_RES = new SplitPathRes();
    private static final FastThreadLocal<SplitPathRes> LOCAL_SPLIT_PATH_RES = new FastThreadLocal<SplitPathRes>() {
        @Override
        protected SplitPathRes initialValue() {
            return new SplitPathRes();
        }
    };
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
        // ZK serialize each string with 4 bytes length prefix, so we add 4 bytes for each string.
        int totalLen = list.stream().mapToInt(String::length).sum() + list.size() * 4;
        maxLenOfListMapping[pathType.ordinal()] = Math.max(maxLenOfListMapping[pathType.ordinal()], totalLen);
    }

    @Override
    public int estimateGetResPayloadLen(String path) {
        return internalEstimateGetResPayloadLen(path) + ZK_PACKET_SYSTEM_PROPS_LEN;
    }

    @Override
    public int estimateGetChildrenResPayloadLen(String path) {
        return internalEstimateGetChildrenResPayloadLen(path) + ZK_PACKET_SYSTEM_PROPS_LEN;
    }

    public int internalEstimateGetResPayloadLen(String path) {
        PathType pathType = getPathType(path);
        int res = maxLenOfGetMapping[pathType.ordinal()];
        return res == UNSET ? DEFAULT_LEN : res;
    }

    public int internalEstimateGetChildrenResPayloadLen(String path) {
        PathType pathType = getPathType(path);
        int res = maxLenOfListMapping[pathType.ordinal()];
        return res == UNSET ? DEFAULT_LEN : res;
    }

    private PathType getPathType(String path) {
        SplitPathRes splitPathRes = splitPath(path);
        if (splitPathRes.partCount < 2) {
            return PathType.OTHERS;
        }
        return switch (splitPathRes.parts[0]) {
            case "admin" -> getAdminPathType(splitPathRes);
            case "managed-ledgers" -> getMlPathType(splitPathRes);
            default -> PathType.OTHERS;
        };
    }

    private PathType getAdminPathType(SplitPathRes splitPathRes) {
        return switch (splitPathRes.parts[1]) {
            case "clusters" -> PathType.CLUSTER;
            case "policies" -> switch (splitPathRes.partCount) {
                case 2 -> PathType.TENANT;
                case 3 -> PathType.NAMESPACE_POLICIES;
                default -> PathType.OTHERS;
            };
            case "partitioned-topics" -> switch (splitPathRes.partCount) {
                case 5 -> PathType.PARTITIONED_NAMESPACE;
                case 6 -> PathType.PARTITIONED_TOPIC;
                default -> PathType.OTHERS;
            };
            default -> PathType.OTHERS;
        };
    }

    private PathType getMlPathType(SplitPathRes splitPathRes) {
        return switch (splitPathRes.partCount) {
            case 4 -> PathType.ML_NAMESPACE;
            case 5 -> PathType.TOPIC;
            // v2 subscription and v1 topic.
            case 6 -> PathType.SUBSCRIPTION;
            // v1 subscription.
            case 7 -> PathType.SUBSCRIPTION;
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

        void reset() {
            parts[0] = null;
            parts[1] = null;
            partCount = 0;
        }
    }

    /**
     * Split the path by the delimiter '/', calculate pieces count and only keep the first two parts.
     */
    static SplitPathRes splitPath(String path) {
        if (path == null || path.length() <= 1) {
            return MEANINGLESS_SPLIT_PATH_RES;
        }
        SplitPathRes res = LOCAL_SPLIT_PATH_RES.get();
        res.reset();
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
        if (start < length) {
            if (count < 2) {
                parts[count] = path.substring(start);
            }
            count++;
        }
        res.partCount = count;
        return res;
    }
}
