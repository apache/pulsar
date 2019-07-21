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

# -*- encoding: utf-8 -*-

"""secretsprovider.py: Interfaces and definitions for Secret Providers
"""
from abc import abstractmethod
import os

class SecretsProvider:
  """Interface for providing secrets information runtime"""
  @abstractmethod
  def init(self, config):
    """Do any kind of initialization"""
    pass

  @abstractmethod
  def provide_secret(self, secret_name, path_to_secret):
    """Fetches the secret located at the path"""
    pass


"""A simple implementation that represents storing secrets in clear text """
class ClearTextSecretsProvider(SecretsProvider):
  def __init__(self):
    pass

  def init(self, config):
    pass

  def provide_secret(self, secret_name, path_to_secret):
    return path_to_secret

"""Implementation that fetches secrets from environment variables"""
class EnvironmentBasedSecretsProvider(SecretsProvider):
  def __init__(self):
    pass

  def init(self, config):
    pass

  def provide_secret(self, secret_name, path_to_secret):
    return os.environ.get(secret_name)
