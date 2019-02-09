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
from pulsar.schema import *
import sys


class MyExample(Record):
    a = String()
    b = Integer(required=True)


service_url = sys.argv[1]
topic = sys.argv[2]

client = pulsar.Client(service_url)

producer = client.create_producer(
                    topic=topic,
                    schema=AvroSchema(MyExample)
                )

producer.send(MyExample(a="Hello", b=1))

client.close()

sys.exit(0)
