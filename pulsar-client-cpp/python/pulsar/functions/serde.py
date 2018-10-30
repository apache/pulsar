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
"""serde.py: SerDe defines the interface for serialization/deserialization.
# Everytime a message is read from pulsar topic, the serde is invoked to
# serialize the bytes into an object before invoking the process method.
# Anytime a python object needs to be written back to pulsar, it is
# serialized into bytes before writing.
"""
from abc import abstractmethod

import pickle

class SerDe(object):
  """Interface for Serialization/Deserialization"""
  @abstractmethod
  def serialize(self, input):
    """Serialize input message into bytes"""
    pass

  @abstractmethod
  def deserialize(self, input_bytes):
    """Serialize input_bytes into an object"""
    pass

class PickleSerDe(SerDe):
  """Pickle based serializer"""
  def serialize(self, input):
      return pickle.dumps(input)

  def deserialize(self, input_bytes):
      return pickle.loads(input_bytes)

class IdentitySerDe(SerDe):
  """Simple Serde that just conversion to string and back"""
  def __init__(self):
    self._types = [int, float, complex, str]

  def serialize(self, input):
    if type(input) in self._types:
      return str(input).encode('utf-8')
    if type(input) == bytes:
      return input
    raise TypeError("IdentitySerde cannot serialize object of type %s" % type(input))

  def deserialize(self, input_bytes):
    for typ in self._types:
      try:
        return typ(input_bytes.decode('utf-8'))
      except:
        pass
    return input_bytes
