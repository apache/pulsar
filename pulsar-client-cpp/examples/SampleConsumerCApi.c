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

#include <stdio.h>
#include <pulsar/c/client.h>

int main() {
    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create("pulsar://localhost:6650", conf);

    pulsar_consumer_configuration_t *consumer_conf = pulsar_consumer_configuration_create();
    pulsar_consumer_configuration_set_consumer_type(consumer_conf, pulsar_ConsumerShared);

    pulsar_consumer_t *consumer;
    pulsar_result res = pulsar_client_subscribe(client, "my-topic", "my-subscrition", consumer_conf, &consumer);
    if (res != pulsar_result_Ok) {
        printf("Failed to create subscribe to topic: %s\n", pulsar_result_str(res));
        return 1;
    }

    for (;;) {
        pulsar_message_t *message;
        res = pulsar_consumer_receive(consumer, &message);
        if (res != pulsar_result_Ok) {
            printf("Failed to receive message: %s\n", pulsar_result_str(res));
            return 1;
        }

        printf("Received message with payload: '%.*s'\n", pulsar_message_get_length(message),
               pulsar_message_get_data(message));

        pulsar_consumer_acknowledge(consumer, message);
        pulsar_message_free(message);
    }

    // Cleanup
    pulsar_consumer_close(consumer);
    pulsar_consumer_free(consumer);
    pulsar_consumer_configuration_free(consumer_conf);

    pulsar_client_close(client);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
}
