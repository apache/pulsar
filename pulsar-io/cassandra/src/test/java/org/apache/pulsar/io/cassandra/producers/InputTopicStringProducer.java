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
package org.apache.pulsar.io.cassandra.producers;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pulsar.client.api.Schema;

@SuppressWarnings({"unchecked", "rawtypes"})
public class InputTopicStringProducer extends InputTopicProducerThread<String> {

    private final Random rnd = new Random();
    int lastReadingId = rnd.nextInt(900000);

    public InputTopicStringProducer(String brokerUrl, String inputTopic) {
        super(brokerUrl, inputTopic);
    }

    @Override
    String getValue() {

        SortedMap<String, Object> elements = new TreeMap();
        elements.put("readingid", lastReadingId++ + "");
        elements.put("avg_ozone", rnd.nextDouble());
        elements.put("min_ozone", rnd.nextDouble());
        elements.put("max_ozone", rnd.nextDouble());
        elements.put("avg_pm10", rnd.nextDouble());
        elements.put("min_pm10", rnd.nextDouble());
        elements.put("max_pm10", rnd.nextDouble());
        elements.put("avg_pm25", rnd.nextDouble());
        elements.put("min_pm25", rnd.nextDouble());
        elements.put("max_pm25", rnd.nextDouble());
        elements.put("local_time_zone", "PST");
        elements.put("state_code", "CA");
        elements.put("reporting_area", lastReadingId + "");
        elements.put("hour_observed", rnd.nextInt(24));
        elements.put("date_observed", "2022-06-18");
        elements.put("latitude", 40.021f);
        elements.put("longitude", -122.33f);

        Gson gson = new Gson();
        Type gsonType = new TypeToken<HashMap>(){}.getType();
        return gson.toJson(elements, gsonType);
    }

    @Override
    Schema getSchema() {
        return Schema.STRING;
    }

}
