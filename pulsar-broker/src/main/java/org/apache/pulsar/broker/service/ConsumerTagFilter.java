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
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import org.apache.pulsar.common.api.proto.PulsarApi;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * TagConsumerFilter - consumer filtering based on tags. Tags are consumer properties with a key
 *  starting with "anytag" or "alltag" and the property as value to match for.
 *  (e.g. "anTag" or "allTag" or "anyTag0", "anyTag1" etc or "allTag0", "allTag1", "allTag2" etc.)
 *  Messages are to be checked for properties with keys starting with "tag" as prefix
 *  (e.g. "tag" or "tag0", "tag1", etc.)
 *  A message is only to be passed if at least one "anyTag" is matched by value against
 *  these message properties (if there are anyTags specified) and all the "allTags" are matched
 *  (if there are some allTags specified).
 */
public class ConsumerTagFilter implements ConsumerFilter {

    private ArrayList<String> anyTags = null;
    private ArrayList<String> allTags = null;

    private boolean initFilter = false;

    public ConsumerTagFilter() {
    }

    /**
     * Inititialize for filtering by collecting filtering tags from the properties.
     * Involves breaking out all consumer properties that are tags into separate lists.
     * If both lists are null then there are no consumer tags to filter messages by.
     *
     * @param metadata the consumer metadata properties.
     */
    public synchronized void initFiltering(Map<String, String> metadata) {
        if (initFilter)
            return;

        ArrayList<String> anyTags = null;
        ArrayList<String> allTags = null;

        for (Map.Entry<String, String> kv : metadata.entrySet()) {
            if (kv.getKey().startsWith("anytag")) {
                if (anyTags == null)
                    anyTags = new ArrayList<>();
                if (!anyTags.contains(kv.getValue()))
                    anyTags.add(kv.getValue());
            }
            if (kv.getKey().startsWith("alltag")) {
                if (allTags == null)
                    allTags = new ArrayList<>();
                if (!allTags.contains(kv.getValue()))
                    allTags.add(kv.getValue());
            }
        }
        if (allTags != null) {
            this.allTags = allTags;
        }
        if (anyTags != null) {
            this.anyTags = anyTags;
        }
        initFilter = true;
    }

    /**
     * Check if filtering.
     * @return true if there are tags to filter by; otherwise false.
     */
    public boolean isFiltering() {
        return anyTags != null || allTags != null;
    }

    /**
     * Filter by given list of message properties.
     * @param properties the message properties to filter by.
     * @param payload the message data buffer.
     * @return true if the message properties contain proterties with "tag" as key prefix
     * and values matching at least one of the filter's anyTag (if any) and all of the allTags.
     */
    @Override
    public boolean filter(List<PulsarApi.KeyValue> properties, ByteBuf payload) {

        if (false && payload != null) {
            byte[] data = new byte[payload.readableBytes()];
            int readIdx = payload.readerIndex();
            payload.readBytes(data);
            payload.readerIndex(readIdx);
            String msg = new String(data);
            System.out.println("ConsumerFiler message: " + msg);
        }

        boolean anyMatch = false;
        HashSet<String> allMatch = null;
        for (PulsarApi.KeyValue kv : properties) {
            // System.out.println("msg prop key=" + kv.getKey() + " value=" + kv.getValue());
            if (kv.getKey().startsWith("tag")) {
                if (anyTags != null && anyTags.contains(kv.getValue())) {
                    // System.out.println("anymatch");
                    anyMatch = true;
                    if (allTags == null)
                        break;
                }
                if (allTags != null && allTags.contains(kv.getValue())) {
                    if (allMatch == null)
                        allMatch = new HashSet<String>();
                    allMatch.add(kv.getValue());
                }
            }
        }
        // Match if we have at least one anyTag (if any anyTags) and all the allTags required.
        return (anyTags == null || anyMatch) &&
                ((allMatch == null ? 0 : allMatch.size()) == (allTags == null ? 0 : allTags.size()));
    }
}
