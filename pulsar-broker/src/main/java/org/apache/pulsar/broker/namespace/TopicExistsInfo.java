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

    private static TopicExistsInfo nonPartitionedExists = new TopicExistsInfo(true, 0);

    private static TopicExistsInfo notExists = new TopicExistsInfo(false, 0);

    public static TopicExistsInfo newPartitionedTopicExists(Integer partitions){
        TopicExistsInfo info = RECYCLER.get();
        info.exists = true;
        info.partitions = partitions.intValue();
        return info;
    }

    public static TopicExistsInfo newNonPartitionedTopicExists(){
        return nonPartitionedExists;
    }

    public static TopicExistsInfo newTopicNotExists(){
        return notExists;
    }

    private final Recycler.Handle<TopicExistsInfo> handle;

    @Getter
    private int partitions;
    @Getter
    private boolean exists;

    private TopicExistsInfo(Recycler.Handle<TopicExistsInfo> handle) {
        this.handle = handle;
    }

    private TopicExistsInfo(boolean exists, int partitions) {
        this.handle = null;
        this.partitions = partitions;
        this.exists = exists;
    }

    public void recycle() {
        if (this == notExists || this == nonPartitionedExists || this.handle == null) {
            return;
        }
        this.exists = false;
        this.partitions = 0;
        this.handle.recycle(this);
    }

    public TopicType getTopicType() {
        return this.partitions > 0 ? TopicType.PARTITIONED : TopicType.NON_PARTITIONED;
    }
}
