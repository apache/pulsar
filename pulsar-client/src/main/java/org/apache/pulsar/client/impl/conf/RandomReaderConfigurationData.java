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
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.Data;
import org.apache.pulsar.client.api.MessagePayloadProcessor;

@Data
public class RandomReaderConfigurationData<T> implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private String topicName;
    private String readerName;
    @JsonIgnore
    private transient MessagePayloadProcessor payloadProcessor;
    private boolean poolMessages;
    private boolean readCommitted = false;
    private SortedMap<String, String> properties = new TreeMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public RandomReaderConfigurationData<T> clone() {
        try {
            RandomReaderConfigurationData<T> clone = (RandomReaderConfigurationData<T>) super.clone();
            clone.properties = new TreeMap<>(properties);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone RandomReaderConfigurationData", e);
        }
    }
}
