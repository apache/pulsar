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
package org.apache.pulsar.common.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.testng.annotations.Test;

public class AvroSchemaCompatTest {

    private static final String COLOR_ENUM = "{\"type\":\"enum\",\"name\":\"Color\","
            + "\"namespace\":\"org.example.shapes\",\"symbols\":[\"RED\",\"BLUE\"]}";

    @Test
    public void testNamedTypeReferencesAreRewrittenInAllSchemaPositions() throws Exception {
        String legacy = "{\"type\":\"record\",\"name\":\"Drawing\",\"namespace\":\"org.example.shapes\",\"fields\":["
                + "{\"name\":\"background\",\"type\":" + COLOR_ENUM + "},"
                + "{\"name\":\"outline\",\"type\":{\"type\":\"org.example.shapes.Color\"}},"
                + "{\"name\":\"palette\",\"type\":{\"type\":\"array\","
                + "\"items\":{\"type\":\"org.example.shapes.Color\"}}},"
                + "{\"name\":\"labels\",\"type\":{\"type\":\"map\","
                + "\"values\":{\"type\":\"org.example.shapes.Color\"}}},"
                + "{\"name\":\"highlight\",\"type\":[\"null\",{\"type\":\"org.example.shapes.Color\"}],"
                + "\"default\":null}"
                + "]}";
        String expected = legacy.replace("{\"type\":\"org.example.shapes.Color\"}", "\"org.example.shapes.Color\"");

        assertJsonEquals(AvroSchemaCompat.normalizeNamedTypeReferences(legacy), expected);
    }

    @Test
    public void testNestedRecordsAreVisited() throws Exception {
        String legacy = "{\"type\":\"record\",\"name\":\"Canvas\",\"namespace\":\"org.example.shapes\",\"fields\":["
                + "{\"name\":\"colors\",\"type\":{\"type\":\"array\",\"items\":" + COLOR_ENUM + "}},"
                + "{\"name\":\"frame\",\"type\":{\"type\":\"record\",\"name\":\"Frame\",\"fields\":["
                + "{\"name\":\"color\",\"type\":{\"type\":\"org.example.shapes.Color\"}}]}}"
                + "]}";
        String expected = legacy.replace("{\"type\":\"org.example.shapes.Color\"}", "\"org.example.shapes.Color\"");

        assertJsonEquals(AvroSchemaCompat.normalizeNamedTypeReferences(legacy), expected);
    }

    @Test
    public void testTopLevelNamedTypeReferenceIsRewritten() {
        assertEquals(AvroSchemaCompat.normalizeNamedTypeReferences("{\"type\":\"org.example.shapes.Color\"}"),
                "\"org.example.shapes.Color\"");
    }

    @Test
    public void testDefaultValuesAreNotModified() throws Exception {
        // the default of "metadata" is a record value that happens to have a field named "type"
        String schema = "{\"type\":\"record\",\"name\":\"Document\",\"fields\":["
                + "{\"name\":\"metadata\",\"type\":{\"type\":\"record\",\"name\":\"Metadata\",\"fields\":["
                + "{\"name\":\"type\",\"type\":\"string\"}]},\"default\":{\"type\":\"org.example.NotASchema\"}},"
                + "{\"name\":\"previous\",\"type\":{\"type\":\"Metadata\"},\"default\":{\"type\":\"draft\"}}"
                + "]}";
        String expected = schema.replace("{\"type\":\"Metadata\"}", "\"Metadata\"");

        assertJsonEquals(AvroSchemaCompat.normalizeNamedTypeReferences(schema), expected);
    }

    @Test
    public void testSchemaWithoutLegacyReferencesIsReturnedUnchanged() {
        String schema = "{\"type\":\"record\",\"name\":\"Point\",\"fields\":["
                + "{\"name\":\"x\",\"type\":\"double\"},{\"name\":\"y\",\"type\":{\"type\":\"double\"}},"
                + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";
        assertSame(AvroSchemaCompat.normalizeNamedTypeReferences(schema), schema);
        assertSame(AvroSchemaCompat.normalizeNamedTypeReferences("\"string\""), "\"string\"");
    }

    @Test
    public void testInvalidJsonIsReturnedUnchanged() {
        String notJson = "{\"type\":\"record\",";
        assertSame(AvroSchemaCompat.normalizeNamedTypeReferences(notJson), notJson);
        assertSame(AvroSchemaCompat.normalizeNamedTypeReferences(null), null);
    }

    private static void assertJsonEquals(String actual, String expected) throws Exception {
        JsonNode actualNode = ObjectMapperFactory.getMapper().reader().readTree(actual);
        JsonNode expectedNode = ObjectMapperFactory.getMapper().reader().readTree(expected);
        assertEquals(actualNode, expectedNode);
    }
}
