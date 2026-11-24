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
package org.apache.pulsar.metadata.impl.batching;

import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.metadata.api.MetadataNodeSizeStats;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ZKConfig;
import org.jctools.queues.MessagePassingQueue;

public class ZKMetadataStoreBatchStrategy implements MetadataStoreBatchStrategy {

    // The headers of response command contains the following attributes, which cost 88 bytes.
    // Base attrs: xid(int), zxid(long), err(int), len(int)
    // Stat attrs: czxid(long), mzxid(long), ctime(long), mtime(long), version(int), cversion(int), aversion(int)
    //             ephemeralOwner(long), dataLength(int), numChildren(int), pzxid(long)
    // By the way, the length of response header may be different between different version, since we had use a half
    // of max size, we can skip to consider the difference.
    public static final int ZK_RESPONSE_HEADER_LEN = 88;
    private final int defaultSize;

    private final int maxOperations;
    private final int maxGetSize;
    private final int maxPutSize;
    private final MetadataNodeSizeStats nodeSizeStats;

    public ZKMetadataStoreBatchStrategy(MetadataNodeSizeStats nodeSizeStats, int maxOperations, int defaultMaxSize,
                                        ZooKeeper zkc) {
        int maxSizeConfigured = zkc.getClientConfig().getInt(
                ZKConfig.JUTE_MAXBUFFER,
                ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT);
        maxSizeConfigured = maxSizeConfigured > 0 ? maxSizeConfigured : defaultMaxSize;
        this.maxOperations = maxOperations;
        this.maxGetSize = maxSizeConfigured;
        this.maxPutSize = maxSizeConfigured;
        this.nodeSizeStats = nodeSizeStats;
        // If the size of the node can not be calculated by "nodeSizeStats", at most package 8 ops into a batch.
        this.defaultSize = Math.max(maxPutSize >>> 4, 1024);
    }

    public int maxSize() {
        return maxPutSize;
    }

    @Override
    public List<MetadataOp> nextBatch(MessagePassingQueue<MetadataOp> opsSrc) {
        int requestSize = 0;
        int estimatedResponseSize = 0;
        // Since the response size is estimated, we use half of the max size to be safe.
        int maxGetSize = this.maxGetSize >>> 1;
        List<MetadataOp> ops = new ArrayList<>();
        while (!opsSrc.isEmpty()) {
            MetadataOp op = opsSrc.peek();
            if (op == null) {
                break;
            }
            MetadataOp.Type type = op.getType();
            String path = op.getPath();
            switch (type) {
                case GET_CHILDREN: {
                    estimatedResponseSize += ZK_RESPONSE_HEADER_LEN;
                    int childrenCount = nodeSizeStats.getMaxChildrenCountOfSameResourceType(path);
                    if (childrenCount < 0) {
                        estimatedResponseSize += defaultSize;
                        break;
                    }
                    // The way that combines list of nodes is as follows
                    // [4 bytes that indicates the length of the next item] [item name].
                    // So we add 4 bytes for each item.
                    int size = nodeSizeStats.getMaxSizeOfSameResourceType(path);
                    if (size > 0) {
                        estimatedResponseSize += (childrenCount * (size + 4));
                    } else {
                        estimatedResponseSize += (childrenCount * (defaultSize + 4));
                    }
                    break;
                }
                case GET: {
                    estimatedResponseSize += ZK_RESPONSE_HEADER_LEN;
                    int size = nodeSizeStats.getMaxSizeOfSameResourceType(path);
                    if (size > 0) {
                        estimatedResponseSize += size;
                    } else {
                        estimatedResponseSize += defaultSize;
                    }
                    break;
                }
                case DELETE:
                case PUT: {
                    requestSize += op.size();
                    // The response of creation contains two attributes: stat and path, so we add them into the
                    // estimation response size.
                    estimatedResponseSize += ZK_RESPONSE_HEADER_LEN;
                    estimatedResponseSize += path.length();
                    break;
                }
                default: {
                    estimatedResponseSize += ZK_RESPONSE_HEADER_LEN;
                    estimatedResponseSize += path.length();
                }
            }
            if (!ops.isEmpty() && (estimatedResponseSize > maxGetSize || requestSize > maxPutSize)) {
                // We have already reached the max size, so flush the current batch.
                break;
            }
            ops.add(opsSrc.poll());
            if (ops.size() == maxOperations) {
                break;
            }
        }
        return ops;
    }
}