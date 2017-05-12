/*
 * Copyright 2016 Yahoo Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.yahoo.pulsar.broker;

/**
 * Class representing data common to bundles.
 */
public class BundleData {
    private long numConsumers;
    private long numProducers;
    private long numTopics;

    private MessageData messageData;

    /**
     * Construct a BundleData.
     */
    public BundleData() {
        messageData = new MessageData();
    }

    public long getNumConsumers() {
        return numConsumers;
    }

    public void setNumConsumers(long numConsumers) {
        this.numConsumers = numConsumers;
    }

    public long getNumProducers() {
        return numProducers;
    }

    public void setNumProducers(long numProducers) {
        this.numProducers = numProducers;
    }

    public long getNumTopics() {
        return numTopics;
    }

    public void setNumTopics(long numTopics) {
        this.numTopics = numTopics;
    }

    public MessageData getMessageData() {
        return messageData;
    }

    public void setMessageData(MessageData messageData) {
        this.messageData = messageData;
    }
}
