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
package org.apache.pulsar.sql.presto.decoder.json;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.json.JsonFieldDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.sql.presto.PulsarRowDecoder;

/**
 * Json PulsarRowDecoder.
 */
public class PulsarJsonRowDecoder implements PulsarRowDecoder {

    private final Map<DecoderColumnHandle, JsonFieldDecoder> fieldDecoders;

    private final GenericJsonSchema genericJsonSchema;

    public PulsarJsonRowDecoder(GenericJsonSchema genericJsonSchema, Set<DecoderColumnHandle> columns) {
        this.genericJsonSchema = requireNonNull(genericJsonSchema, "genericJsonSchema is null");
        this.fieldDecoders = columns.stream().collect(toImmutableMap(identity(), PulsarJsonFieldDecoder::new));
    }

    private static JsonNode locateNode(JsonNode tree, DecoderColumnHandle columnHandle) {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());
        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
    }

    /**
     * decode ByteBuf by {@link org.apache.pulsar.client.api.schema.GenericSchema}.
     * @param byteBuf
     * @return
     */
    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(ByteBuf byteBuf) {
        GenericJsonRecord record = (GenericJsonRecord) genericJsonSchema.decode(byteBuf);
        JsonNode tree = record.getJsonNode();
        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();
        for (Map.Entry<DecoderColumnHandle, JsonFieldDecoder> entry : fieldDecoders.entrySet()) {
            DecoderColumnHandle columnHandle = entry.getKey();
            JsonFieldDecoder decoder = entry.getValue();
            JsonNode node = locateNode(tree, columnHandle);
            decodedRow.put(columnHandle, decoder.decode(node));
        }
        return Optional.of(decodedRow);
    }

    private static final Logger log = Logger.get(PulsarJsonRowDecoderFactory.class);
}
