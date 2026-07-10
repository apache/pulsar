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
sys.modules['bookkeeper'] = Mock()
sys.modules['bookkeeper.types'] = Mock()
sys.modules['bookkeeper.common'] = Mock()
sys.modules['bookkeeper.common.exceptions'] = Mock()
sys.modules['bookkeeper.proto'] = Mock()
sys.modules['bookkeeper.proto.stream_pb2'] = Mock()

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
    if not hasattr(sys.stdout, 'logger'):
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

class TestPropertiesForwarding(unittest.TestCase):

  def _setup_mock_instance(self, forward_property):
    function_details = Function_pb2.FunctionDetails()
    function_details.sink.topic = "test_sink_topic"
    function_details.sink.forwardSourceMessageProperty = forward_property
    
    mock_pulsar_client = Mock()
    mock_producer = Mock()
    mock_pulsar_client.create_producer.return_value = mock_producer
    
    instance = PythonInstance('test_instance', 'test_func', '1.0', function_details, 100, 30, 'user_code', mock_pulsar_client, Mock(), 'test_cluster', 'test_url', None)
    instance.producer = mock_producer
    instance.contextimpl = Mock()
    instance.contextimpl.get_message_partition_index.return_value = None
    instance.output_schema = "DEFAULT_SCHEMA"
    instance.output_serde = Mock()
    instance.output_serde.serialize.return_value = b'serialized_output'
    instance.effectively_once = False
    
    return instance, mock_producer

  def test_forwards_properties(self):
    instance, mock_producer = self._setup_mock_instance(forward_property=True)
    
    mock_msg = Mock()
    mock_msg.topic = "source-topic"
    mock_msg.message.message_id().serialize.return_value = b'msg-id'
    mock_msg.message.properties.return_value = {"custom-key": "custom-value"}
    
    instance.process_result("output-data", mock_msg)
    
    args, kwargs = mock_producer.send_async.call_args
    self.assertIn("custom-key", kwargs['properties'])
    self.assertEqual(kwargs['properties']["custom-key"], "custom-value")
    self.assertIn("__pfn_input_topic__", kwargs['properties'])

  def test_do_not_forward_properties(self):
    instance, mock_producer = self._setup_mock_instance(forward_property=False)
    
    mock_msg = Mock()
    mock_msg.topic = "source-topic"
    mock_msg.message.message_id().serialize.return_value = b'msg-id'
    mock_msg.message.properties.return_value = {"custom-key": "custom-value"}
    
    instance.process_result("output-data", mock_msg)
    
    args, kwargs = mock_producer.send_async.call_args
    self.assertNotIn("custom-key", kwargs['properties'])
    self.assertIn("__pfn_input_topic__", kwargs['properties'])

