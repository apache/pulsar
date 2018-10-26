
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
    context_impl = ContextImpl(instance_config, logger, pulsar_client, user_code, consumers)

    context_impl.publish("test_topic_name", "test_message")

    producer.send_async.assert_called_with("test_message", None, properties=None)



