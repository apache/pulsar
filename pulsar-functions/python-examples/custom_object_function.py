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


from pulsar import Function, SerDe

class MyObject(object):
  def __init__(self):
    self.a = 0
    self.b = 0

class CustomSerDe(SerDe):
  def __init__(self):
    pass

  def serialize(self, object):
    return ("%d,%d" % (object.a, object.b)).encode('utf-8')

  def deserialize(self, input_bytes):
    split = str(input_bytes.decode()).split(',')
    retval = MyObject()
    retval.a = int(split[0])
    retval.b = int(split[1])
    return retval

# Function that deals with custom objects
class CustomObjectFunction(Function):
  def __init__(self):
    pass

  def process(self, input, context):
    retval = MyObject()
    retval.a = input.a + 11
    retval.b = input.b + 24
    return retval