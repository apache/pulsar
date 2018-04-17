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
package org.apache.pulsar.connect.core;

/**
 * Pulsar Connect's Message interface. Message encapsulates the
 * information about a message being read/written from/to a Source/Sink.
 */
public class Message<T> {
    // The partition information if any of the message
    private String partitionId;
    // The sequence id of the message
    private Long sequenceId;
    // The actual data of the message
    private T data;

    public Message(String partitionId, Long sequenceId, T data) {
        this.partitionId = partitionId;
        this.sequenceId = sequenceId;
        this.data = data;
    }

    public Message(T data) {
        this.partitionId = "";
        this.sequenceId = -1L;
        this.data = data;
    }

    public String getPartitionId() { return this.partitionId; }
    public Long getSequenceId() { return this.sequenceId; }
    public T getData() { return this.data; }
}