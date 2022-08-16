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
#include <iostream>
#include <pulsar/Client.h>
#include <lib/LogUtils.h>

DECLARE_LOG_OBJECT()

using namespace pulsar;

int main() {
    Client client("pulsar://localhost:6650");

    std::string jsonSchema =
        "{\"type\":\"record\",\"name\":\"cpx\",\"fields\":[{\"name\":\"re\",\"type\":\"double\"},{\"name\":"
        "\"im\",\"type\":\"double\"}]}";

    SchemaInfo keySchema(JSON, "key-json", jsonSchema);
    SchemaInfo valueSchema(JSON, "value-json", jsonSchema);
    SchemaInfo keyValueSchema(keySchema, valueSchema, SEPARATED);
    ConsumerConfiguration consumerConfiguration;
    consumerConfiguration.setSchema(keyValueSchema);

    Consumer consumer;
    Result result = client.subscribe("persistent://public/default/kv-schema", "consumer-1",
                                     consumerConfiguration, consumer);
    if (result != ResultOk) {
        LOG_ERROR("Failed to subscribe: " << result);
        return -1;
    }

    LOG_INFO("Start receive message.")

    Message msg;
    while (true) {
        consumer.receive(msg);
        LOG_INFO("Received: " << msg << "  with payload '" << msg.getDataAsString() << "'");
        LOG_INFO("Received: " << msg << "  with partitionKey '" << msg.getPartitionKey() << "'");
        KeyValue keyValue = msg.getKeyValueData(SEPARATED);
        LOG_INFO("Received: " << msg << "  with key '" << keyValue.getKeyData() << "'");
        LOG_INFO("Received: " << msg << "  with value '" << keyValue.getValueData() << "'");
        consumer.acknowledge(msg);
    }

    client.close();
}
