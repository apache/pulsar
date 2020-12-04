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


DEFAULT_CLIENT_TOPIC = 'rpc-client-topic'
DEFAULT_SERVER_TOPIC = 'rpc-server-topic'


class RPCServer(object):
    def __init__(self,
                 client_topic=DEFAULT_CLIENT_TOPIC,
                 server_topic=DEFAULT_SERVER_TOPIC):
        self.client_topic = client_topic
        self.server_topic = server_topic

        self.client = pulsar.Client('pulsar://localhost:6650')
        self.producer = self.client.create_producer(client_topic)
        self.consumer = \
            self.client.subscribe(server_topic,
                                  'rpc-server',
                                  pulsar.ConsumerType.Shared,
                                  message_listener=self.on_response)

    def on_response(self, consumer, message):
        print('Received from {0}: {1}'.format(message.partition_key(),
                                              message.data().decode('utf-8')))

        self.producer.send('{} bar'.format(message.data().decode('utf-8')),
                           partition_key=message.partition_key())
        consumer.acknowledge(message)

    def start(self):
        self.consumer.resume_message_listener()


rpc_server = RPCServer()
rpc_server.start()

try:
    while True:
        pass
except KeyboardInterrupt:
    print('Interrupted.')
