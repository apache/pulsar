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

import backtype.storm.spout.ISpoutOutputCollector;
import org.apache.pulsar.client.api.Message;

import java.util.ArrayList;
import java.util.List;

public class MockSpoutOutputCollector implements ISpoutOutputCollector {

    private boolean emitted = false;
    private Message lastMessage = null;
    private String data = null;
    private String streamId = null;

    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        emitted = true;
        data = (String) tuple.get(0);
        lastMessage = (Message) messageId;
        this.streamId = streamId;
        return new ArrayList<Integer>();
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        emitted = true;
        data = (String) tuple.get(0);
        this.streamId = streamId;
        lastMessage = (Message) messageId;
    }

    @Override
    public void reportError(Throwable error) {
    }

    public boolean emitted() {
        return emitted;
    }

    public String getTupleData() {
        return data;
    }

    public Message getLastMessage() {
        return lastMessage;
    }

    public String getStreamId() {
        return streamId;
    }

    public void reset() {
        emitted = false;
        data = null;
        lastMessage = null;
        streamId = null;
    }
}
