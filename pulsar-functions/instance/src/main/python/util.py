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

"""util.py: Some misc utility functions
"""
import os
import inspect
import sys
import importlib
from threading import Timer

import log

Log = log.Log
PULSAR_API_ROOT = 'pulsar'
PULSAR_FUNCTIONS_API_ROOT = 'functions'

def import_class(from_path, full_class_name):
  from_path = str(from_path)
  full_class_name = str(full_class_name)
  try:
    return import_class_from_path(from_path, full_class_name)
  except Exception as e:
    our_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    api_dir = os.path.join(our_dir, PULSAR_API_ROOT, PULSAR_FUNCTIONS_API_ROOT)
    try:
      return import_class_from_path(api_dir, full_class_name)
    except Exception as e:
      Log.info("Failed to import class %s from path %s" % (full_class_name, from_path))
      Log.info(e, exc_info=True)
      return None

def import_class_from_path(from_path, full_class_name):
  Log.debug('Trying to import %s from path %s' % (full_class_name, from_path))
  split = full_class_name.split('.')
  classname_path = '.'.join(split[:-1])
  class_name = full_class_name.split('.')[-1]
  if from_path not in sys.path:
    Log.debug("Add a new dependency to the path: %s" % from_path)
    sys.path.insert(0, from_path)
  if not classname_path:
    mod = importlib.import_module(class_name)
    return mod
  else:
    mod = importlib.import_module(classname_path)
    retval = getattr(mod, class_name)
    return retval

def getFullyQualifiedFunctionName(tenant, namespace, name):
  return "%s/%s/%s" % (tenant, namespace, name)

def getFullyQualifiedInstanceId(tenant, namespace, name, instance_id):
    return "%s/%s/%s:%s" % (tenant, namespace, name, instance_id)

def get_properties(fullyQualifiedName, instanceId):
    return {"application": "pulsar-function", "id": str(fullyQualifiedName), "instance_id": str(instanceId)}

class FixedTimer():

    def __init__(self, t, hFunction, name="timer-thread"):
        self.t = t
        self.hFunction = hFunction
        self.thread = Timer(self.t, self.handle_function)
        self.thread.setName(name)
        self.thread.setDaemon(True)

    def handle_function(self):
        self.hFunction()
        self.thread = Timer(self.t, self.handle_function)
        self.thread.start()

    def start(self):
        self.thread.start()

    def cancel(self):
        self.thread.cancel()