package org.apache.pulsar.io.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class SerDe {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Deserialize a JSON payload into Jackson's JsonNode tree.
     *
     * @param input the JSON bytes
     * @return the root JsonNode
     * @throws IOException if parsing fails
     */
    public JsonNode deserialize(byte[] input) throws IOException {
        return MAPPER.readTree(input);
    }

    /**
     * Serialize a JsonNode tree into UTF-8 JSON bytes.
     *
     * @param node the JsonNode to write out
     * @return UTF-8 encoded JSON bytes
     * @throws JsonProcessingException if serialization fails
     */
    public byte[] serialize(JsonNode node) throws JsonProcessingException {
        return MAPPER.writeValueAsBytes(node);
    }
}