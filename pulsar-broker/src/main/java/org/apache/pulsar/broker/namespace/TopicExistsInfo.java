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
package org.apache.pulsar.broker.namespace;

import io.netty.util.Recycler;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.TopicType;

public class TopicExistsInfo {

    private static final Recycler<TopicExistsInfo> RECYCLER = new Recycler<>() {
        @Override
        protected TopicExistsInfo newObject(Handle<TopicExistsInfo> handle) {
            return new TopicExistsInfo(handle);
        }
    };

    private static TopicExistsInfo nonPartitionedExists = new TopicExistsInfo(null);
    static {
        nonPartitionedExists.exists = true;
        nonPartitionedExists.topicType = TopicType.NON_PARTITIONED;
        nonPartitionedExists.partitions = null;
    }

    private static TopicExistsInfo notExists = new TopicExistsInfo(null);
    static {
        notExists.exists = false;
        notExists.topicType = TopicType.NON_PARTITIONED;
        notExists.partitions = null;
    }

    public static TopicExistsInfo partitionedExists(Integer partitions){
        return newInstance(true, TopicType.PARTITIONED, partitions);
    }

    public static TopicExistsInfo nonPartitionedExists(){
        return nonPartitionedExists;
    }

    public static TopicExistsInfo notExists(){
        return notExists;
    }

    private static TopicExistsInfo newInstance(boolean exists, TopicType topicType, Integer partitions){
        TopicExistsInfo info = RECYCLER.get();
        info.exists = exists;
        info.topicType = topicType;
        if (topicType == null) {
            throw new IllegalArgumentException("The param topicType can not be null when creating a TopicExistsInfo"
                    + " obj.");
        }
        if (topicType.equals(TopicType.PARTITIONED)) {
            if (partitions == null || partitions.intValue() < 1) {
                throw new IllegalArgumentException("The param partitions can not be null or less than 1 when creating"
                        + " a partitioned TopicExistsInfo obj.");
            }
            info.partitions = partitions.intValue();
        } else {
            if (partitions != null) {
                throw new IllegalArgumentException("The param partitions must be null when creating a non-partitioned"
                        + " TopicExistsInfo obj.");
            }
        }
        return info;
    }

    private final Recycler.Handle<TopicExistsInfo> handle;

    @Getter
    private TopicType topicType;
    @Getter
    private Integer partitions;
    @Getter
    private boolean exists;

    private TopicExistsInfo(Recycler.Handle<TopicExistsInfo> handle) {
        this.handle = handle;
    }

    public void recycle(){
        if (this == notExists || this == nonPartitionedExists || this.handle == null) {
            return;
        }
        this.exists = false;
        this.topicType = null;
        this.partitions = null;
        this.handle.recycle(this);
    }
}
