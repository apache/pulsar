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
import java.util.concurrent.atomic.AtomicReferenceArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataNodeSizeStats;

@Slf4j
public class DefaultMetadataNodeSizeStats implements MetadataNodeSizeStats {

    public static final int UNSET = -1;

    private static final SplitPathRes MEANINGLESS_SPLIT_PATH_RES = new SplitPathRes();
    private static final FastThreadLocal<SplitPathRes> LOCAL_SPLIT_PATH_RES = new FastThreadLocal<SplitPathRes>() {
        @Override
        protected SplitPathRes initialValue() {
            return new SplitPathRes();
        }
    };
    private final AtomicReferenceArray<Integer> maxSizeMapping;
    private final AtomicReferenceArray<Integer> maxChildrenCountMapping;

   public DefaultMetadataNodeSizeStats() {
        int pathTypeCount = PathType.values().length;
        maxSizeMapping = new AtomicReferenceArray<>(pathTypeCount);
        maxChildrenCountMapping = new AtomicReferenceArray<>(pathTypeCount);
        for (int i = 0; i < pathTypeCount; i++) {
            maxSizeMapping.set(i, UNSET);
            maxChildrenCountMapping.set(i, UNSET);
        }
    }

    @Override
    public void recordPut(String path, byte[] data) {
        PathType pathType = getPathType(path);
        if (pathType == PathType.UNKNOWN) {
            return;
        }
        maxSizeMapping.set(pathType.ordinal(), Math.max(maxSizeMapping.get(pathType.ordinal()), data.length));
    }

    @Override
    public void recordGetRes(String path, GetResult getResult) {
        PathType pathType = getPathType(path);
        if (pathType == PathType.UNKNOWN || getResult == null) {
            return;
        }
        maxSizeMapping.set(pathType.ordinal(), Math.max(maxSizeMapping.get(pathType.ordinal()),
                getResult.getValue().length));
    }

    @Override
    public void recordGetChildrenRes(String path, List<String> list) {
        PathType pathType = getPathType(path);
        if (pathType == PathType.UNKNOWN) {
            return;
        }
        int size = CollectionUtils.isEmpty(list) ? 0 : list.size();
        maxChildrenCountMapping.set(pathType.ordinal(), Math.max(maxChildrenCountMapping.get(pathType.ordinal()),
                size));
    }

    @Override
    public int getMaxSizeOfSameResourceType(String path) {
        PathType pathType = getPathType(path);
        if (pathType == PathType.UNKNOWN) {
            return -1;
        }
        return maxSizeMapping.get(pathType.ordinal());
    }

    @Override
    public int getMaxChildrenCountOfSameResourceType(String path) {
        PathType pathType = getPathType(path);
        if (pathType == PathType.UNKNOWN) {
            return -1;
        }
        return maxChildrenCountMapping.get(pathType.ordinal());
    }

    private PathType getPathType(String path) {
        SplitPathRes splitPathRes = splitPath(path);
        if (splitPathRes.partCount < 2) {
            return PathType.UNKNOWN;
        }
        return switch (splitPathRes.parts[0]) {
            case "admin" -> getAdminPathType(splitPathRes);
            case "managed-ledgers" -> getMlPathType(splitPathRes);
            case "loadbalance" -> getLoadBalancePathType(splitPathRes);
            case "namespace" -> getBundleOwnerPathType(splitPathRes);
            case "schemas" -> getSchemaPathType(splitPathRes);
            default -> PathType.UNKNOWN;
        };
    }

    private PathType getAdminPathType(SplitPathRes splitPathRes) {
        return switch (splitPathRes.parts[1]) {
            case "clusters" -> PathType.CLUSTER;
            case "policies" -> switch (splitPathRes.partCount) {
                case 3 -> PathType.TENANT;
                case 4 -> PathType.NAMESPACE_POLICIES;
                default -> PathType.UNKNOWN;
            };
            case "local-policies" -> switch (splitPathRes.partCount) {
                case 4 -> PathType.NAMESPACE_POLICIES;
                default -> PathType.UNKNOWN;
            };
            case "partitioned-topics" -> switch (splitPathRes.partCount) {
                case 5 -> PathType.PARTITIONED_NAMESPACE;
                case 6 -> PathType.PARTITIONED_TOPIC;
                default -> PathType.UNKNOWN;
            };
            default -> PathType.UNKNOWN;
        };
    }

    private PathType getBundleOwnerPathType(SplitPathRes splitPathRes) {
       return switch (splitPathRes.partCount) {
           case 3 -> PathType.BUNDLE_OWNER_NAMESPACE;
           case 4 -> PathType.BUNDLE_OWNER;
           default -> PathType.UNKNOWN;
       };
    }

    private PathType getSchemaPathType(SplitPathRes splitPathRes) {
       if (splitPathRes.partCount == 4) {
           return PathType.TOPIC_SCHEMA;
       }
       return PathType.UNKNOWN;
    }

    private PathType getLoadBalancePathType(SplitPathRes splitPathRes) {
        return switch (splitPathRes.parts[1]) {
            case "brokers" -> switch (splitPathRes.partCount) {
                case 2 -> PathType.BROKERS;
                case 3 -> PathType.BROKER;
                default -> PathType.UNKNOWN;
            };
            case "bundle-data" -> switch (splitPathRes.partCount) {
                case 4 -> PathType.BUNDLE_NAMESPACE;
                case 5 -> PathType.BUNDLE_DATA;
                default -> PathType.UNKNOWN;
            };
            case "broker-time-average" -> switch (splitPathRes.partCount) {
                case 3 -> PathType.BROKER_TIME_AVERAGE;
                default -> PathType.UNKNOWN;
            };
            case "leader" -> PathType.BROKER_LEADER;
            default -> PathType.UNKNOWN;
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
            default -> PathType.UNKNOWN;
        };
    }

    enum PathType {
        // admin
        CLUSTER,
        TENANT,
        NAMESPACE_POLICIES,
        LOCAL_POLICIES,
        // load-balance
        BROKERS,
        BROKER,
        BROKER_LEADER,
        BUNDLE_NAMESPACE,
        BUNDLE_DATA,
        BROKER_TIME_AVERAGE ,
        BUNDLE_OWNER_NAMESPACE ,
        BUNDLE_OWNER ,
        // topic schema
        TOPIC_SCHEMA,
        // partitioned topics.
        PARTITIONED_TOPIC,
        PARTITIONED_NAMESPACE,
        // managed ledger.
        ML_NAMESPACE,
        TOPIC,
        SUBSCRIPTION,
        UNKNOWN;
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
