#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import pulsar


client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                    'my-topic',
                    block_if_queue_full=True,
                    batching_enabled=True,
                    batching_max_publish_delay_ms=10,
                    properties={
                        "producer-name": "test-producer-name",
                        "producer-id": "test-producer-id"
                    }
                )

for i in range(10):
    try:
        producer.send('hello'.encode('utf-8'), None)
    except Exception as e:
        print("Failed to send message: %s", e)

producer.flush()
producer.close()
