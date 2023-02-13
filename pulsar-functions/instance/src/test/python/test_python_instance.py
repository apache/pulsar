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

from mock import Mock
import sys
sys.modules['prometheus_client'] = Mock()

from contextimpl import ContextImpl
from python_instance import PythonInstance, InstanceConfig
from pulsar import Message

import Function_pb2
import log
import os
import unittest

class TestContextImpl(unittest.TestCase):

  def Any(cls):
    class Any(cls):
      def __eq__(self, other):
        return True
    return Any()

  def setUp(self):
    log.init_logger("INFO", "foo", os.environ.get("PULSAR_HOME") + "/conf/functions-logging/console_logging_config.ini")

  def test_context_publish(self):
    instance_id = 'test_instance_id'
    function_id = 'test_function_id'
    function_version = 'test_function_version'
    function_details = Function_pb2.FunctionDetails()
    max_buffered_tuples = 100
    instance_config = InstanceConfig(instance_id, function_id, function_version, function_details, max_buffered_tuples)
    logger = log.Log
    pulsar_client = Mock()
    producer = Mock()
    producer.send_async = Mock(return_value=None)
    pulsar_client.create_producer = Mock(return_value=producer)
    user_code = __file__
    consumers = None
    context_impl = ContextImpl(instance_config, logger, pulsar_client, user_code, consumers, None, None, None, None)

    msg = Message()
    msg.message_id = Mock(return_value="test_message_id")
    msg.partition_key = Mock(return_value="test_key")
    context_impl.set_current_message_context(msg, "test_topic_name")

    context_impl.publish("test_topic_name", "test_message")

    args, kwargs = producer.send_async.call_args
    self.assertEqual(args[0].decode("utf-8"), "test_message")
    self.assertEqual(args[1].args[1], "test_topic_name")
    self.assertEqual(args[1].args[2], "test_message_id")

  def test_context_ack_partitionedtopic(self):
    instance_id = 'test_instance_id'
    function_id = 'test_function_id'
    function_version = 'test_function_version'
    function_details = Function_pb2.FunctionDetails()
    max_buffered_tuples = 100
    instance_config = InstanceConfig(instance_id, function_id, function_version, function_details, max_buffered_tuples)
    logger = log.Log
    pulsar_client = Mock()
    user_code = __file__
    consumer = Mock()
    consumer.acknowledge = Mock(return_value=None)
    consumers = {"mytopic" : consumer}
    context_impl = ContextImpl(instance_config, logger, pulsar_client, user_code, consumers, None, None, None, None)
    context_impl.ack("test_message_id", "mytopic-partition-3")

    args, kwargs = consumer.acknowledge.call_args
    self.assertEqual(args[0], "test_message_id")

  def test_efffectively_once(self):
    instance_id = 'test_instance_id'
    function_id = 'test_function_id'
    function_version = 'test_function_version'
    # fill out function_details
    function_details = Function_pb2.FunctionDetails()
    function_details.processingGuarantees = Function_pb2.ProcessingGuarantees.Value('EFFECTIVELY_ONCE')
    function_details.sink.topic = "persistent://public/default/output"
    max_buffered_tuples = 100
    pulsar_client = Mock()
    user_code = __file__
    py_instance_impl = PythonInstance(
      instance_id=instance_id,
      function_id=function_id,
      function_version=function_version,
      function_details=function_details,
      max_buffered_tuples=max_buffered_tuples,
      expected_healthcheck_interval=100,
      secrets_provider=None,
      cluster_name="",
      state_storage_serviceurl="",
      config_file=None,
      user_code=user_code,
      pulsar_client=pulsar_client)

    self.assertEqual(py_instance_impl.effectively_once, True)
