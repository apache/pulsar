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

#include <pulsar/c/client.h>

#include <stdio.h>
#include <string.h>

int main() {
    pulsar_client_configuration_t *conf = pulsar_client_configuration_create();
    pulsar_client_t *client = pulsar_client_create("pulsar://localhost:6650", conf);

    pulsar_producer_configuration_t* producer_conf = pulsar_producer_configuration_create();
    pulsar_producer_configuration_set_batching_enabled(producer_conf, 1);
    pulsar_producer_t *producer;

    pulsar_result err = pulsar_client_create_producer(client, "my-topic", producer_conf, &producer);
    if (err != pulsar_result_Ok) {
        printf("Failed to create producer: %s\n", pulsar_result_str(err));
        return 1;
    }

    for (int i = 0; i < 10; i++) {
        const char* data = "my-content";
        pulsar_message_t* message = pulsar_message_create();
        pulsar_message_set_content(message, data, strlen(data));

        err = pulsar_producer_send(producer, message);
        if (err == pulsar_result_Ok) {
            printf("Sent message %d\n", i);
        } else {
            printf("Failed to publish message: %s\n", pulsar_result_str(err));
            return 1;
        }

        pulsar_message_free(message);
    }

    // Cleanup
    pulsar_producer_close(producer);
    pulsar_producer_free(producer);
    pulsar_producer_configuration_free(producer_conf);

    pulsar_client_close(client);
    pulsar_client_free(client);
    pulsar_client_configuration_free(conf);
}
