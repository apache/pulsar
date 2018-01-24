package org.apache.pulsar.common.schema;

public enum SchemaType {
    AVRO("avro"),
    PROTOBUF("protobuf"),
    THRIFT("thrift"),
    JSON("json");

    private final String str;

    SchemaType(String str) {
        this.str = str;
    }
}
