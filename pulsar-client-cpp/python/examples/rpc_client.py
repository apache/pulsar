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
import threading
import uuid


DEFAULT_CLIENT_TOPIC = 'rpc-client-topic'
DEFAULT_SERVER_TOPIC = 'rpc-server-topic'
UUID = str(uuid.uuid4())
NUM_CLIENT = 0
LOCK = threading.Lock()


class RPCClient(object):

    def __init__(self,
                 client_topic=DEFAULT_CLIENT_TOPIC,
                 server_topic=DEFAULT_SERVER_TOPIC):
        self.client_topic = client_topic
        self.server_topic = server_topic

        global NUM_CLIENT
        with LOCK:
            self.client_no = NUM_CLIENT
            NUM_CLIENT += 1

        self.response = None
        self.partition_key = '{0}_{1}'.format(UUID, self.client_no)
        self.client = pulsar.Client('pulsar://localhost:6650')
        self.producer = self.client.create_producer(server_topic)
        self.consumer = \
            self.client.subscribe(client_topic,
                                  'rpc-client-{}'.format(self.partition_key),
                                  message_listener=self.on_response)

        self.consumer.resume_message_listener()

    def on_response(self, consumer, message):
        if message.partition_key() == self.partition_key \
           and consumer.topic() == self.client_topic:
            msg = message.data().decode('utf-8')
            print('Received: {0}'.format(msg))
            self.response = msg
            consumer.acknowledge(message)

    def call(self, message):
        self.response = None
        self.producer.send(message.encode('utf-8'), partition_key=self.partition_key)

        while self.response is None:
            pass

        return self.response


msg = 'foo'
rpc_client = RPCClient()
ret = rpc_client.call(msg)

print('RPCClient message sent: {0}, result: {1}'.format(msg, ret))
