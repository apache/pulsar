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


# DEPENDENCIES:  unittest2
import python_instance_main

import os
import log
import unittest

class TestContextImpl(unittest.TestCase):

  def Any(cls):
    class Any(cls):
      def __eq__(self, other):
        return True
    return Any()

  def setUp(self):
    log.init_logger("INFO", "foo", os.environ.get("PULSAR_HOME") + "/conf/functions-logging/console_logging_config.ini")

  def test_arguments(self):
    parser = python_instance_main.generate_arguments_parser()
    argv = [
      "--function_details", "test_function_details",
      "--py", "test_py",
      "--instance_id", "test_instance_id",
      "--function_id", "test_function_id",
      "--function_version", "test_function_version",
      "--pulsar_serviceurl", "test_pulsar_serviceurl",
      "--client_auth_plugin", "test_client_auth_plugin",
      "--client_auth_params", "test_client_auth_params",
      "--tls_allow_insecure_connection", "true",
      "--hostname_verification_enabled", "true",
      "--tls_trust_cert_path", "test_tls_trust_cert_path",
      "--port", "1000",
      "--metrics_port", "1001",
      "--max_buffered_tuples", "100",
      "--config_file", "test_python_runtime_config.ini"
    ]
    args = parser.parse_args(argv)
    python_instance_main.merge_arguments(args, args.config_file)
    # argument from command line test
    self.assertEqual(args.function_details, "test_function_details")
    # argument from config file test
    self.assertEqual(args.use_tls, "true")
    # argument read priority test
    self.assertEqual(args.port, 1000)
    # mandatory argument test
    self.assertEqual(args.expected_healthcheck_interval, "50")
    # optional argument test
    self.assertEqual(args.secrets_provider, None)
