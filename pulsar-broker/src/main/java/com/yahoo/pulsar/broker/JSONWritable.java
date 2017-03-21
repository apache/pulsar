package com.yahoo.pulsar.broker;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;

/**
 * Helper class used to conveniently convert a data class to a JSON.
 */
public class JSONWritable {

    /**
     * Get the JSON of this object as a byte[].
     * @return A byte[] of this object's JSON.
     * @throws JsonProcessingException
     */
    @JsonIgnore
    public byte[] getJsonBytes() throws JsonProcessingException {
        return ObjectMapperFactory.getThreadLocal().writeValueAsBytes(this);
    }

    /**
     * Get the JSON of this object as a String.
     * @return A String of this object's JSON.
     * @throws JsonProcessingException
     */
    @JsonIgnore
    public String getJsonString() throws JsonProcessingException {
        return ObjectMapperFactory.getThreadLocal().writeValueAsString(this);
    }
}
