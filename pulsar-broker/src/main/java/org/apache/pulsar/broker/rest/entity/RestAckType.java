package org.apache.pulsar.broker.rest.entity;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum RestAckType {
    SINGLE, CUMULATIVE
}
