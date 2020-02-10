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

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Values;

public interface MessageToValuesMapper extends Serializable {

    /**
     * Convert {@link org.apache.pulsar.client.api.Message} to tuple values.
     *
     * @param msg
     * @return
     */
    Values toValues(Message<byte[]> msg);

    /**
     * Declare the output schema for the spout.
     *
     * @param declarer
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
}
