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
package org.apache.pulsar.client.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.Murmur3_32Hash;

/***
 * Test message-key whether match client-current-consumer.
 */
public interface StickyKeyConsumerPredicate extends Predicate<String> {

    String SPECIAL_CONSUMER_MARK = "__special_consumer_mark__";

    String OTHER_CONSUMER_MARK = "_";

    /** TODO serialize without dependencies. **/
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static int transferToHashKey(String key){
        return Murmur3_32Hash.getInstance().makeHash(key.getBytes(StandardCharsets.UTF_8));
    }

    default boolean test(String key) {
        return SPECIAL_CONSUMER_MARK.equals(select(transferToHashKey(key)));
    }

    default String encode() {
        // TODO implements
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            // All props are fixed, so this case never occur
            return "{}";
        }
    }

    static StickyKeyConsumerPredicate decode(String bytes){
        // TODO implements
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(bytes);
            int predicateType = jsonNode.get("predicateType").intValue();
            switch (predicateType){
                case 1:{
                    int lowHash = jsonNode.get("lowHash").intValue();
                    int highHash = jsonNode.get("highHash").intValue();
                    int rangeSize = jsonNode.get("rangeSize").intValue();
                    return new Predicate4HashRangeAutoSplitStickyKeyConsumerSelector(lowHash, highHash, rangeSize);
                }
                case 2:{
                    int rangeSize = jsonNode.get("rangeSize").intValue();
                    JsonNode rangeMapNode = jsonNode.get("rangeMap");
                    NavigableMap<Integer, String> rangeMap =
                            OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rangeMapNode),
                            new TypeReference<TreeMap<Integer, String>>() {});
                    return new Predicate4HashRangeExclusiveStickyKeyConsumerSelector(rangeMap, rangeSize);
                }
                case 3: {
                    JsonNode rangeMapNode = jsonNode.get("hashRing");
                    NavigableMap<Integer, List<String>> hashRing =
                            OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rangeMapNode),
                            new TypeReference<TreeMap<Integer, List<String>>>() {});
                    return new Predicate4ConsistentHashingStickyKeyConsumerSelector(hashRing);
                }
                default:{
                    return DENIED;
                }
            }
        } catch (JsonProcessingException | ClassCastException e) {
            StickyKeyConsumerPredicateLogger.log.error(new StringBuilder("In Key_Shared mode, Deserialize fail: ")
                    .append(bytes).toString(), e);
            return DENIED;
        }
    }

    String select(int hash);

    DeniedStickyKeyConsumerPredicate DENIED = new DeniedStickyKeyConsumerPredicate();

    class DeniedStickyKeyConsumerPredicate implements StickyKeyConsumerPredicate {

        @Override
        public String select(int hash) {
            return null;
        }
    }

    @Data
    class Predicate4HashRangeAutoSplitStickyKeyConsumerSelector implements StickyKeyConsumerPredicate {

        private int predicateType = 1;

        private final int lowHash;

        private final int highHash;

        private final int rangeSize;

        public Predicate4HashRangeAutoSplitStickyKeyConsumerSelector(
                int lowHash, int highHash, int rangeSize) {
            this.lowHash = lowHash;
            this.highHash = highHash;
            this.rangeSize = rangeSize;
        }

        @Override
        public String select(int hash) {
            int slot = hash % rangeSize;
            if (lowHash < slot && slot <= highHash) {
                return SPECIAL_CONSUMER_MARK;
            }
            return OTHER_CONSUMER_MARK;
        }
    }

    @Data
    @ToString
    class Predicate4HashRangeExclusiveStickyKeyConsumerSelector implements StickyKeyConsumerPredicate {

        private int predicateType = 2;

        protected final NavigableMap<Integer, String> rangeMap;

        protected final int rangeSize;

        public Predicate4HashRangeExclusiveStickyKeyConsumerSelector(NavigableMap<Integer, String> rangeMap
                , int rangeSize){
            this.rangeMap = rangeMap;
            this.rangeSize = rangeSize;
        }

        @Override
        public String select(int hash) {
            if (rangeMap.size() > 0) {
                int slot = hash % rangeSize;
                Map.Entry<Integer, String> ceilingEntry = rangeMap.ceilingEntry(slot);
                Map.Entry<Integer, String> floorEntry = rangeMap.floorEntry(slot);
                String ceilingConsumer = ceilingEntry != null ? ceilingEntry.getValue() : null;
                String floorConsumer = floorEntry != null ? floorEntry.getValue() : null;
                if (floorConsumer != null && floorConsumer.equals(ceilingConsumer)) {
                    return ceilingConsumer;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    @Data
    @ToString
    class Predicate4ConsistentHashingStickyKeyConsumerSelector implements StickyKeyConsumerPredicate{

        private int predicateType = 3;

        private final NavigableMap<Integer, List<String>> hashRing;

        public Predicate4ConsistentHashingStickyKeyConsumerSelector(NavigableMap<Integer, List<String>> hashRing){
            this.hashRing = hashRing;
        }

        @Override
        public String select(int hash) {
            if (hashRing.isEmpty()) {
                return null;
            }
            List<String> consumerList;
            Map.Entry<Integer, List<String>> ceilingEntry = hashRing.ceilingEntry(hash);
            if (ceilingEntry != null) {
                consumerList =  ceilingEntry.getValue();
            } else {
                consumerList = hashRing.firstEntry().getValue();
            }
            return consumerList.get(hash % consumerList.size());
        }
    }

    @Slf4j
    class StickyKeyConsumerPredicateLogger{

    }
}
