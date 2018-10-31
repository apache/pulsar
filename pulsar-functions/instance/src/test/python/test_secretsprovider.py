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

from secretsprovider import ClearTextSecretsProvider
from secretsprovider import EnvironmentBasedSecretsProvider

import log
import os
import unittest

class TestContextImpl(unittest.TestCase):

  def setUp(self):
    log.init_logger("INFO", "foo", os.environ.get("PULSAR_HOME") + "/conf/functions-logging/console_logging_config.ini")

  def test_cleartext_secretsprovider(self):
    provider = ClearTextSecretsProvider()
    secret = provider.provide_secret("secretName", "secretPath")
    self.assertEqual(secret, "secretPath")
    secret = provider.provide_secret("secretName", "")
    self.assertEqual(secret, "")
    secret = provider.provide_secret("secretName", None)
    self.assertEqual(secret, None)

  def test_environment_secretsprovider(self):
    provider = EnvironmentBasedSecretsProvider()
    secret = provider.provide_secret("secretName", "secretPath")
    self.assertEqual(secret, None)
    os.environ["secretName"] = "secretValue"
    secret = provider.provide_secret("secretName", "")
    self.assertEqual(secret, "secretValue")
    secret = provider.provide_secret("secretName", None)
    self.assertEqual(secret, "secretValue")
    secret = provider.provide_secret("secretName", "somethingelse")
    self.assertEqual(secret, "secretValue")