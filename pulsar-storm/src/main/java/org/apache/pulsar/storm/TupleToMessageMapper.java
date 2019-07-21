/**
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
package org.apache.pulsar.storm;

import java.io.Serializable;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public interface TupleToMessageMapper extends Serializable {

    /**
     * Convert tuple to {@link org.apache.pulsar.client.api.Message}.
     *
     * @param tuple
     * @return
     * @deprecated use {@link #toMessage(TypedMessageBuilder, Tuple)}
     */
    @Deprecated
    default Message<byte[]> toMessage(Tuple tuple) {
        return null;
    }

    /**
     * Set the value on a message builder to prepare the message to be published from the Bolt.
     *
     * @param tuple
     * @return
     */
    default TypedMessageBuilder<byte[]> toMessage(TypedMessageBuilder<byte[]> msgBuilder, Tuple tuple) {
        // Default implementation provided for backward compatibility
        Message<byte[]> msg = toMessage(tuple);
        msgBuilder.value(msg.getData())
            .properties(msg.getProperties());
        if (msg.hasKey()) {
            msgBuilder.key(msg.getKey());
        }
        return msgBuilder;
    }


    /**
     * Declare the output schema for the bolt.
     *
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer);
}
