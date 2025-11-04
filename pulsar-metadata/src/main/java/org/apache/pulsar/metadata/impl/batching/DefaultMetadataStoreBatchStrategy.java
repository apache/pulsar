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
import org.jctools.queues.MessagePassingQueue;

/***
 * The default batching strategy, which only consider how the max operations and max size work for the request packet.
 * And do not care about the response packet.
 */
public class DefaultMetadataStoreBatchStrategy implements MetadataStoreBatchStrategy {

    private final int maxOperations;
    private final int maxPutSize;

    public DefaultMetadataStoreBatchStrategy(int maxOperations, int maxPutSize) {
        this.maxOperations = maxOperations;
        this.maxPutSize = maxPutSize;
    }

    @Override
    public List<MetadataOp> nextBatch(MessagePassingQueue<MetadataOp> opsSrc) {
        int requestSize = 0;
        List<MetadataOp> ops = new ArrayList<>();
        while (!opsSrc.isEmpty()) {
            MetadataOp op = opsSrc.peek();
            if (op == null) {
                break;
            }
            MetadataOp.Type type = op.getType();
            switch (type) {
                case PUT:
                case DELETE: {
                    requestSize += op.size();
                    break;
                }
                default: {}
            }
            if (!ops.isEmpty() && requestSize > maxPutSize) {
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