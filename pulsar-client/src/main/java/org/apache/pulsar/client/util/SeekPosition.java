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
package org.apache.pulsar.client.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

public class SeekPosition {
    public final static String seekPositionLatest = "latest";
    public final static String seekPositionEarliest = "earliest";
    public final static String seekPositionDefault = seekPositionLatest;

    private final static Map<String, MessageId> positions;
    
    static {
        HashMap<String, MessageId> _positions = new HashMap<String, MessageId>();
        _positions.put(seekPositionLatest, MessageIdImpl.latest);
        _positions.put(seekPositionEarliest, MessageIdImpl.earliest);
        positions = Collections.unmodifiableMap(_positions);
    }

    
    private static SeekPosition _instance;
    public static MessageId getPosition(String key){
        if(positions.containsKey(key)){
            return positions.get(key);
        }
        return positions.get(seekPositionDefault);
    }
}