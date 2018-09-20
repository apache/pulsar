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


from pulsar import Function

# Example function that uses the built in publish function in the context
# to publish to a desired topic based on config
class PublishFunction(Function):
  def __init__(self):
    pass

  def process(self, input, context):
    publish_topic = "publishtopic"
    if "publish-topic" in context.get_user_config_map():
      publish_topic = context.get_user_config_value("publish-topic")
    context.publish(publish_topic, input + '!')
    return
