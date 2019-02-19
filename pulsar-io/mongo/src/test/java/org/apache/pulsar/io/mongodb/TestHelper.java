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

package org.apache.pulsar.io.mongodb;

import java.util.HashMap;
import java.util.Map;

public final class TestHelper {

    public static final String URI = "mongodb://localhost";

    public static final String DB = "pulsar";

    public static final String COLL = "messages";

    public static final int BATCH_SIZE = 2;

    public static final int BATCH_TIME = 500;


    public static Map<String, Object> createMap(boolean full) {
        final Map<String, Object> map = new HashMap<>();
        map.put("mongoUri", URI);
        map.put("database", DB);

        if (full) {
            map.put("collection", COLL);
            map.put("batchSize", BATCH_SIZE);
            map.put("batchTimeMs", BATCH_TIME);
        }

        return map;
    }

    private TestHelper() {

    }
}
