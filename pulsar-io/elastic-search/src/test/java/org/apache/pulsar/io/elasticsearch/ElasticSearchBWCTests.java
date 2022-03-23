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
package org.apache.pulsar.io.elasticsearch;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;

public class ElasticSearchBWCTests {

    @Test
    public void testGenericRecord() throws Exception {
        String json = "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}";

        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of("elasticSearchUrl", "http://localhost:9200", "schemaEnable", "true"), null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(new Record<GenericObject>() {
            @Override
            public GenericObject getValue() {
                return new GenericObject() {
                    @Override
                    public SchemaType getSchemaType() {
                        return SchemaType.BYTES;
                    }

                    @Override
                    public Object getNativeObject() {
                        return json.getBytes(StandardCharsets.UTF_8);
                    }
                };
            }
        });
        assertEquals(pair.getLeft(), null);
        assertEquals(pair.getRight(), json);
    }
}
