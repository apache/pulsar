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


# DEPENDENCIES:  unittest2,mock

from contextimpl import ContextImpl
from python_instance import InstanceConfig
from mock import Mock

import Function_pb2
import log
import os
import unittest

class TestContextImpl(unittest.TestCase):

  def setUp(self):
    log.init_logger("INFO", "foo", os.environ.get("PULSAR_HOME") + "/conf/functions-logging/console_logging_config.ini")

  def test_context_publish(self):
    instance_id = 'test_instance_id'
    function_id = 'test_function_id'
    function_version = 'test_function_version'
    function_details = Function_pb2.FunctionDetails()
    max_buffered_tuples = 100;
    instance_config = InstanceConfig(instance_id, function_id, function_version, function_details, max_buffered_tuples)
    logger = log.Log
    pulsar_client = Mock()
    producer = Mock()
    producer.send_async = Mock(return_value=None)
    pulsar_client.create_producer = Mock(return_value=producer)
    user_code=__file__
    consumers = None
    context_impl = ContextImpl(instance_config, logger, pulsar_client, user_code, consumers, None)

    context_impl.publish("test_topic_name", "test_message")

    producer.send_async.assert_called_with("test_message", None, properties=None)



