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

import _pulsar
import io
import enum

from .schema import Schema

try:
    import fastavro
    HAS_AVRO = True
except ModuleNotFoundError:
    HAS_AVRO = False

if HAS_AVRO:
    class AvroSchema(Schema):
        def __init__(self, record_cls):
            super(AvroSchema, self).__init__(record_cls, _pulsar.SchemaType.AVRO,
                                             record_cls.schema(), 'AVRO')
            self._schema = record_cls.schema()

        def _get_serialized_value(self, x):
            if isinstance(x, enum.Enum):
                return x.name
            else:
                return x

        def encode(self, obj):
            self._validate_object_type(obj)
            buffer = io.BytesIO()
            m = {k: self._get_serialized_value(v) for k, v in obj.__dict__.items()}
            fastavro.schemaless_writer(buffer, self._schema, m)
            return buffer.getvalue()

        def decode(self, data):
            buffer = io.BytesIO(data)
            d = fastavro.schemaless_reader(buffer, self._schema)
            return self._record_cls(**d)

else:
    class AvroSchema(Schema):
        def __init__(self, _record_cls):
            raise Exception("Avro library support was not found. Make sure to install Pulsar client " +
                            "with Avro support: pip3 install 'pulsar-client[avro]'")

        def encode(self, obj):
            pass

        def decode(self, data):
            pass
