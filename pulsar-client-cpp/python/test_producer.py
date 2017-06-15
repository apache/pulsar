#!/usr/bin/env python
#
# Copyright 2016 Yahoo Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pulsar


client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                    'persistent://sample/standalone/ns/my-topic',
                    block_if_queue_full=True,
                    batching_enabled=True,
                    batching_max_publish_delay_ms=10
                )

while True:
    try:
        producer.send_async('hello', None)
    except Exception as e:
        print("Failed to send message: %s", e)

producer.close()
