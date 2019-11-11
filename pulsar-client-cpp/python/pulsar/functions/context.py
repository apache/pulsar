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
"""context.py: Context defines context information available during
# processing of a request.
"""
from abc import abstractmethod

class Context(object):
  """Interface defining information available at process time"""
  @abstractmethod
  def get_message_id(self):
    """Return the messageid of the current message that we are processing"""
    pass

  @abstractmethod
  def get_message_key(self):
    """Return the key of the current message that we are processing"""
    pass

  @abstractmethod
  def get_message_eventtime(self):
    """Return the event time of the current message that we are processing"""
    pass

  @abstractmethod
  def get_message_properties(self):
    """Return the message properties kv map of the current message that we are processing"""
    pass

  @abstractmethod
  def get_current_message_topic_name(self):
    """Returns the topic name of the message that we are processing"""
    pass
  
  @abstractmethod
  def get_function_tenant(self):
    """Returns the tenant of the message that's being processed"""
    pass

  @abstractmethod
  def get_function_namespace(self):
    """Returns the namespace of the message that's being processed"""

  @abstractmethod
  def get_function_name(self):
    """Returns the function name that we are a part of"""
    pass

  @abstractmethod
  def get_function_id(self):
    """Returns the function id that we are a part of"""
    pass

  @abstractmethod
  def get_instance_id(self):
    """Returns the instance id that is executing the function"""
    pass

  @abstractmethod
  def get_function_version(self):
    """Returns the version of function that we are executing"""
    pass

  @abstractmethod
  def get_logger(self):
    """Returns the logger object that can be used to do logging"""
    pass

  @abstractmethod
  def get_user_config_value(self, key):
    """Returns the value of the user-defined config. If the key doesn't exist, None is returned"""
    pass
  
  @abstractmethod
  def get_user_config_map(self):
    """Returns the entire user-defined config as a dict (the dict will be empty if no user-defined config is supplied)"""
    pass

  @abstractmethod
  def get_secret(self, secret_name):
    """Returns the secret value associated with the name. None if nothing was found"""
    pass

  @abstractmethod
  def get_partition_key(self):
    """Returns partition key of the input message is one exists"""
    pass


  @abstractmethod
  def record_metric(self, metric_name, metric_value):
    """Records the metric_value. metric_value has to satisfy isinstance(metric_value, numbers.Number)"""
    pass

  @abstractmethod
  def publish(self, topic_name, message, serde_class_name="serde.IdentitySerDe", properties=None, compression_type=None, callback=None, message_conf=None):
    """Publishes message to topic_name by first serializing the message using serde_class_name serde
    The message will have properties specified if any

    The available options for message_conf:

      properties,
      partition_key,
      sequence_id,
      replication_clusters,
      disable_replication,
      event_timestamp

    """
    pass

  @abstractmethod
  def get_input_topics(self):
    """Returns the input topics of function"""
    pass

  @abstractmethod
  def get_output_topic(self):
    """Returns the output topic of function"""
    pass

  @abstractmethod
  def get_output_serde_class_name(self):
    """return output Serde class"""
    pass

  @abstractmethod
  def ack(self, msgid, topic):
    """ack this message id"""
    pass

  @abstractmethod
  def incr_counter(self, key, amount):
    """incr the counter of a given key in the managed state"""
    pass

  @abstractmethod
  def get_counter(self, key):
    """get the counter of a given key in the managed state"""
    pass

  @abstractmethod
  def del_counter(self, key):
    """delete the counter of a given key in the managed state"""
    pass

  @abstractmethod
  def put_state(self, key, value):
    """update the value of a given key in the managed state"""
    pass

  @abstractmethod
  def get_state(self, key):
    """get the value of a given key in the managed state"""
    pass
