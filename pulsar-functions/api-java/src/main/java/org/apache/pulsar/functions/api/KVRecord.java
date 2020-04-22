package org.apache.pulsar.functions.api;

import org.apache.pulsar.client.api.Schema;

public interface KVRecord<K, V> extends Record {

    Schema<K> getKeySchema();

    Schema<V> getValueSchema();

}
