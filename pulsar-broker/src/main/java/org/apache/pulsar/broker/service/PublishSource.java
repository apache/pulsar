package org.apache.pulsar.broker.service;

import lombok.Value;

@Value
public class PublishSource {
    TransportCnx cnx;
    Topic topic;
}
