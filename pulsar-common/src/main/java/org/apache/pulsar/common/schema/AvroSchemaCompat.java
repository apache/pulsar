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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.Set;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Compatibility handling for Avro schema definitions that were written for Avro versions before 1.12.2.
 *
 * <p>Up to Avro 1.12.1 the parser accepted a reference to a previously defined named type written as a JSON
 * object whose {@code type} attribute is the name of the type, for example {@code {"type": "org.example.Color"}},
 * and resolved it to the named type while ignoring any other attribute of the object. Avro 1.12.2 rejects this
 * form (AVRO-4176) and only accepts the bare name {@code "org.example.Color"}. Schema definitions in this form
 * have been accepted and stored by the schema registry, and clients keep producing them, so they have to be
 * rewritten to the bare name before they are passed to the Avro parser.
 */
public final class AvroSchemaCompat {

    /**
     * The values of {@code type} for which an object defines a schema. For any other value the object is a
     * reference to a named type.
     */
    private static final Set<String> TYPE_KEYWORDS = Set.of(
            "null", "boolean", "int", "long", "float", "double", "bytes", "string",
            "record", "error", "enum", "array", "map", "fixed");

    private AvroSchemaCompat() {
    }

    /**
     * Rewrite named type references written as {@code {"type": "name"}} objects to the bare {@code "name"}.
     *
     * <p>Only positions that hold a schema are visited: the schema itself, field types, array items, map values
     * and union members. Other JSON values, for example field defaults, are never modified.
     *
     * @param schemaDefinition the Avro schema definition as JSON
     * @return the rewritten definition, or the given string unchanged if it contains no such reference or is not
     *         valid JSON
     */
    public static String normalizeNamedTypeReferences(String schemaDefinition) {
        if (schemaDefinition == null || schemaDefinition.indexOf('{') < 0) {
            return schemaDefinition;
        }
        JsonNode root;
        try {
            root = ObjectMapperFactory.getMapper().reader().readTree(schemaDefinition);
        } catch (JsonProcessingException e) {
            // let the Avro parser report the error
            return schemaDefinition;
        }
        NamedTypeReferenceRewriter rewriter = new NamedTypeReferenceRewriter();
        JsonNode rewritten = rewriter.rewrite(root);
        if (!rewriter.changed) {
            return schemaDefinition;
        }
        try {
            return ObjectMapperFactory.getMapper().writer().writeValueAsString(rewritten);
        } catch (JsonProcessingException e) {
            return schemaDefinition;
        }
    }

    private static final class NamedTypeReferenceRewriter {

        private boolean changed;

        JsonNode rewrite(JsonNode node) {
            if (node instanceof ArrayNode) {
                ArrayNode union = (ArrayNode) node;
                for (int i = 0; i < union.size(); i++) {
                    union.set(i, rewrite(union.get(i)));
                }
                return union;
            }
            if (!(node instanceof ObjectNode)) {
                // a bare name, or something invalid that the Avro parser reports
                return node;
            }
            ObjectNode object = (ObjectNode) node;
            JsonNode type = object.get("type");
            if (type == null || !type.isTextual()) {
                return object;
            }
            String typeName = type.textValue();
            if (!TYPE_KEYWORDS.contains(typeName)) {
                changed = true;
                return TextNode.valueOf(typeName);
            }
            switch (typeName) {
                case "record":
                case "error":
                    rewriteFieldTypes(object.get("fields"));
                    break;
                case "array":
                    rewriteChild(object, "items");
                    break;
                case "map":
                    rewriteChild(object, "values");
                    break;
                default:
                    // primitive, enum and fixed types have no nested schema
                    break;
            }
            return object;
        }

        private void rewriteFieldTypes(JsonNode fields) {
            if (fields == null || !fields.isArray()) {
                return;
            }
            for (JsonNode field : fields) {
                if (field instanceof ObjectNode) {
                    rewriteChild((ObjectNode) field, "type");
                }
            }
        }

        private void rewriteChild(ObjectNode object, String name) {
            JsonNode child = object.get(name);
            if (child != null) {
                object.set(name, rewrite(child));
            }
        }
    }
}
