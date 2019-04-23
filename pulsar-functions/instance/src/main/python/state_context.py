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

"""state_context.py: state context for accessing managed state
"""
from abc import abstractmethod
from bookkeeper import admin, kv
from bookkeeper.common.exceptions import NamespaceNotFoundError, StreamNotFoundError, KeyNotFoundError
from bookkeeper.proto import stream_pb2
from bookkeeper.proto.stream_pb2 import HASH
from bookkeeper.proto.stream_pb2 import TABLE
from bookkeeper.types import StorageClientSettings


def new_bk_table_conf(num_ranges):
    """Create a table configuration with the specified `num_ranges`"""
    return stream_pb2.StreamConfiguration(
        key_type=HASH,
        min_num_ranges=num_ranges,
        initial_num_ranges=num_ranges,
        split_policy=stream_pb2.SplitPolicy(
            type=stream_pb2.SplitPolicyType.values()[0],
            fixed_range_policy=stream_pb2.FixedRangeSplitPolicy(
                num_ranges=2
            )
        ),
        rolling_policy=stream_pb2.SegmentRollingPolicy(
            size_policy=stream_pb2.SizeBasedSegmentRollingPolicy(
                max_segment_size=128 * 1024 * 1024
            )
        ),
        retention_policy=stream_pb2.RetentionPolicy(
            time_policy=stream_pb2.TimeBasedRetentionPolicy(
                retention_minutes=-1
            )
        ),
        storage_type=TABLE
    )


def create_state_context(state_storage_serviceurl, table_ns, table_name):
    """Create the state context based on state storage serviceurl"""
    if state_storage_serviceurl is None:
        return NullStateContext()
    else:
        return BKManagedStateContext(state_storage_serviceurl, table_ns, table_name)


class StateContext(object):
    """Interface defining operations on managed state"""

    @abstractmethod
    def incr(self, key, amount):
        pass

    @abstractmethod
    def put(self, key, value):
        pass

    @abstractmethod
    def get_value(self, key):
        pass

    @abstractmethod
    def get_amount(self, key):
        pass


class NullStateContext(StateContext):
    """A state context that does nothing"""

    def incr(self, key, amount):
        return

    def put(self, key, value):
        return

    def get_value(self, key):
        return None

    def get_amount(self, key):
        return None


class BKManagedStateContext(StateContext):
    """A state context that access bookkeeper managed state"""

    def __init__(self, state_storage_serviceurl, table_ns, table_name):
        client_settings = StorageClientSettings(
            service_uri=state_storage_serviceurl)
        admin_client = admin.client.Client(
            storage_client_settings=client_settings)
        # create namespace and table if needed
        ns = admin_client.namespace(table_ns)
        try:
            ns.get(stream_name=table_name)
        except NamespaceNotFoundError:
            admin_client.namespaces().create(namespace=table_ns)
            # TODO: make number of table ranges configurable
            table_conf = new_bk_table_conf(1)
            ns.create(
                stream_name=table_name,
                stream_config=table_conf)
        except StreamNotFoundError:
            # TODO: make number of table ranges configurable
            table_conf = new_bk_table_conf(1)
            ns.create(
                stream_name=table_name,
                stream_config=table_conf)
        self.__client__ = kv.Client(storage_client_settings=client_settings,
                                    namespace=table_ns)
        self.__table__ = self.__client__.table(table_name=table_name)

    def incr(self, key, amount):
        return self.__table__.incr_str(key, amount)

    def get_amount(self, key):
        try:
            kv = self.__table__.get_str(key)
            if kv is not None:
                return kv.number_value
            else:
                return None
        except KeyNotFoundError:
            return None

    def get_value(self, key):
        try:
            kv = self.__table__.get_str(key)
            if kv is not None:
                return kv.value
            else:
                return None
        except KeyNotFoundError:
            return None

    def put(self, key, value):
        return self.__table__.put_str(key, value)

    def delete_key(self, key):
        return self.__table__.delete_str(key)
