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
package org.apache.pulsar.zookeeper;

import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;

/**
 * Provides a set of simple {@link Deserializer} for deserializing commonly used data types from ZooKeeper.
 * 
 */
public class Deserializers {

    public static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();
    public static final Deserializer<Policies> POLICY_DESERIALIZER = new PolicyDeserializer();

    /**
     * Deserializes data received from ZK into Java Strings. Constructs a new String by decoding the specified array of
     * bytes using the platform's default charset.
     */
    public static final class StringDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(String key, byte[] content) throws Exception {
            return new String(content);
        }
    }

    /**
     * Deserializes data received from Zookeeper into policies object.
     */
    public static final class PolicyDeserializer implements Deserializer<Policies> {
        @Override
        public Policies deserialize(String key, byte[] content) throws Exception {
            return ObjectMapperFactory.getThreadLocal().readValue(content, Policies.class);
        }
    }

    // Add other common data types like int etc here when needed
}
