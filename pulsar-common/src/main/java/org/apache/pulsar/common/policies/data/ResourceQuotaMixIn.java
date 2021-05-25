package org.apache.pulsar.common.policies.data;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class ResourceQuotaMixIn {
    @JsonIgnore
    abstract public boolean isValid();
}
