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

from unittest import TestCase, main

import fastavro
import pulsar
from pulsar.schema import *
from enum import Enum
import json
from fastavro.schema import load_schema


class SchemaTest(TestCase):

    serviceUrl = 'pulsar://localhost:6650'

    def test_simple(self):
        class Color(Enum):
            red = 1
            green = 2
            blue = 3

        class Example(Record):
            _sorted_fields = True
            a = String()
            b = Integer()
            c = Array(String())
            d = Color
            e = Boolean()
            f = Float()
            g = Double()
            h = Bytes()
            i = Map(String())
            j = CustomEnum(Color)

        fastavro.parse_schema(Example.schema())
        self.assertEqual(Example.schema(), {
            "name": "Example",
            "type": "record",
            "fields": [
                {"name": "a", "type": ["null", "string"]},
                {"name": "b", "type": ["null", "int"]},
                {"name": "c", "type": ["null", {
                    "type": "array",
                    "items": "string"}]
                },
                {"name": "d",
                 "type": ["null", {
                    "type": "enum",
                    "name": "Color",
                    "symbols": ["red", "green", "blue"]}]
                 },
                {"name": "e", "type": ["null", "boolean"]},
                {"name": "f", "type": ["null", "float"]},
                {"name": "g", "type": ["null", "double"]},
                {"name": "h", "type": ["null", "bytes"]},
                {"name": "i", "type": ["null", {
                    "type": "map",
                    "values": "string"}]
                },
                {"name": "j", "type": ["null", "Color"]}
            ]
        })

    def test_complex(self):
        class Color(Enum):
            red = 1
            green = 2
            blue = 3

        class MySubRecord(Record):
            _sorted_fields = True
            x = Integer()
            y = Long()
            z = String()
            color = CustomEnum(Color)

        class Example(Record):
            _sorted_fields = True
            a = String()
            sub = MySubRecord     # Test with class
            sub2 = MySubRecord()  # Test with instance

        fastavro.parse_schema(Example.schema())
        self.assertEqual(Example.schema(), {
            "name": "Example",
            "type": "record",
            "fields": [
                {"name": "a", "type": ["null", "string"]},
                {"name": "sub",
                 "type": ["null", {
                     "name": "MySubRecord",
                     "type": "record",
                     "fields": [
                        {'name': 'color', 'type': ['null', {'type': 'enum', 'name': 'Color', 'symbols':
                            ['red', 'green', 'blue']}]},
                        {"name": "x", "type": ["null", "int"]},
                        {"name": "y", "type": ["null", "long"]},
                        {"name": "z", "type": ["null", "string"]}]
                 }]
                 },
                 {"name": "sub2",
                  "type": ["null", 'MySubRecord']
                 }
            ]
        })

        def test_complex_with_required_fields(self):
            class MySubRecord(Record):
                x = Integer(required=True)
                y = Long(required=True)
                z = String()

            class Example(Record):
                a = String(required=True)
                sub = MySubRecord(required=True)

            self.assertEqual(Example.schema(), {
                "name": "Example",
                "type": "record",
                "fields": [
                    {"name": "a", "type": "string"},
                    {"name": "sub",
                     "type": {
                         "name": "MySubRecord",
                         "type": "record",
                         "fields": [{"name": "x", "type": "int"},
                                    {"name": "y", "type": "long"},
                                    {"name": "z", "type": ["null", "string"]}]
                     }
                     },
                ]
            })

    def test_invalid_enum(self):
        class Color:
            red = 1
            green = 2
            blue = 3

        class InvalidEnum(Record):
            a = Integer()
            b = Color

        # Enum will be ignored
        self.assertEqual(InvalidEnum.schema(),
                         {'name': 'InvalidEnum', 'type': 'record',
                          'fields': [{'name': 'a', 'type': ["null", 'int']}]})

    def test_initialization(self):
        class Example(Record):
            a = Integer()
            b = Integer()

        r = Example(a=1, b=2)
        self.assertEqual(r.a, 1)
        self.assertEqual(r.b, 2)

        r.b = 5

        self.assertEqual(r.b, 5)

        # Setting non-declared field should fail
        try:
            r.c = 3
            self.fail('Should have failed')
        except AttributeError:
            # Expected
            pass

        try:
            Record(a=1, c=8)
            self.fail('Should have failed')
        except AttributeError:
            # Expected
            pass

        except TypeError:
            # Expected
            pass

    def _expectTypeError(self, func):
        try:
            func()
            self.fail('Should have failed')
        except TypeError:
            # Expected
            pass

    def test_field_type_check(self):
        class Example(Record):
            a = Integer()
            b = String(required=False)

        self._expectTypeError(lambda:  Example(a=1, b=2))

        class E2(Record):
            a = Boolean()

        E2(a=False)  # ok
        self._expectTypeError(lambda:  E2(a=1))

        class E3(Record):
            a = Float()

        E3(a=1.0)  # Ok
        self._expectTypeError(lambda:  E3(a=1))

        class E4(Record):
            a = Null()

        E4(a=None)  # Ok
        self._expectTypeError(lambda:  E4(a=1))

        class E5(Record):
            a = Long()

        E5(a=1234)  # Ok
        self._expectTypeError(lambda:  E5(a=1.12))

        class E6(Record):
            a = String()

        E6(a="hello")  # Ok
        self._expectTypeError(lambda:  E5(a=1.12))

        class E6(Record):
            a = Bytes()

        E6(a="hello".encode('utf-8'))  # Ok
        self._expectTypeError(lambda:  E5(a=1.12))

        class E7(Record):
            a = Double()

        E7(a=1.0)  # Ok
        self._expectTypeError(lambda:  E3(a=1))

        class Color(Enum):
            red = 1
            green = 2
            blue = 3

        class OtherEnum(Enum):
            red = 1
            green = 2
            blue = 3

        class E8(Record):
            a = Color

        e = E8(a=Color.red)  # Ok
        self.assertEqual(e.a, Color.red)

        e = E8(a='red')  # Ok
        self.assertEqual(e.a, Color.red)

        e = E8(a=1)  # Ok
        self.assertEqual(e.a, Color.red)

        self._expectTypeError(lambda:  E8(a='redx'))
        self._expectTypeError(lambda: E8(a=OtherEnum.red))
        self._expectTypeError(lambda: E8(a=5))

        class E9(Record):
            a = Array(String())

        E9(a=['a', 'b', 'c'])  # Ok
        self._expectTypeError(lambda:  E9(a=1))
        self._expectTypeError(lambda: E9(a=[1, 2, 3]))
        self._expectTypeError(lambda: E9(a=['1', '2', 3]))

        class E10(Record):
            a = Map(Integer())

        E10(a={'a': 1, 'b': 2})  # Ok
        self._expectTypeError(lambda:  E10(a=1))
        self._expectTypeError(lambda: E10(a={'a': '1', 'b': 2}))
        self._expectTypeError(lambda: E10(a={1: 1, 'b': 2}))

        class SubRecord1(Record):
            s = Integer()

        class SubRecord2(Record):
            s = String()

        class E11(Record):
            a = SubRecord1

        E11(a=SubRecord1(s=1))  # Ok
        self._expectTypeError(lambda:  E11(a=1))
        self._expectTypeError(lambda: E11(a=SubRecord2(s='hello')))

    def test_field_type_check_defaults(self):
        try:
            class Example(Record):
                a = Integer(default="xyz")

            self.fail("Class declaration should have failed")
        except TypeError:
            pass # Expected

    def test_serialize_json(self):
        class Example(Record):
            a = Integer()
            b = Integer()

        self.assertEqual(Example.schema(), {
            "name": "Example",
            "type": "record",
            "fields": [
                {"name": "a", "type": ["null", "int"]},
                {"name": "b", "type": ["null", "int"]},
            ]
        })

        s = JsonSchema(Example)
        r = Example(a=1, b=2)
        data = s.encode(r)
        self.assertEqual(json.loads(data), {'a': 1, 'b': 2})

        r2 = s.decode(data)
        self.assertEqual(r2.__class__.__name__, 'Example')
        self.assertEqual(r2, r)

    def test_serialize_avro(self):
        class Example(Record):
            a = Integer()
            b = Integer()

        self.assertEqual(Example.schema(), {
            "name": "Example",
            "type": "record",
            "fields": [
                {"name": "a", "type": ["null", "int"]},
                {"name": "b", "type": ["null", "int"]},
            ]
        })

        s = AvroSchema(Example)
        r = Example(a=1, b=2)
        data = s.encode(r)

        r2 = s.decode(data)
        self.assertEqual(r2.__class__.__name__, 'Example')
        self.assertEqual(r2, r)

    def test_non_sorted_fields(self):
        class T1(Record):
            a = Integer()
            b = Integer()
            c = Double()
            d = String()

        class T2(Record):
            b = Integer()
            a = Integer()
            d = String()
            c = Double()

        self.assertNotEqual(T1.schema()['fields'], T2.schema()['fields'])

    def test_sorted_fields(self):
        class T1(Record):
            _sorted_fields = True
            a = Integer()
            b = Integer()

        class T2(Record):
            _sorted_fields = True
            b = Integer()
            a = Integer()

        self.assertEqual(T1.schema()['fields'], T2.schema()['fields'])

    def test_schema_version(self):
        class Example(Record):
            a = Integer()
            b = Integer()

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        'my-avro-python-schema-version-topic',
                        schema=AvroSchema(Example))

        consumer = client.subscribe('my-avro-python-schema-version-topic', 'sub-1',
                                    schema=AvroSchema(Example))

        r = Example(a=1, b=2)
        producer.send(r)

        msg = consumer.receive()

        self.assertIsNotNone(msg.schema_version())

        self.assertEquals(b'\x00\x00\x00\x00\x00\x00\x00\x00', msg.schema_version().encode())

        self.assertEqual(r, msg.value())

        client.close()

    def test_serialize_wrong_types(self):
        class Example(Record):
            a = Integer()
            b = Integer()

        class Foo(Record):
            x = Integer()
            y = Integer()

        s = JsonSchema(Example)
        try:
            data = s.encode(Foo(x=1, y=2))
            self.fail('Should have failed')
        except TypeError:
            pass  # expected

        try:
            data = s.encode('hello')
            self.fail('Should have failed')
        except TypeError:
            pass  # expected

    def test_defaults(self):
        class Example(Record):
            a = Integer(default=5)
            b = Integer()
            c = String(default='hello')

        r = Example()
        self.assertEqual(r.a, 5)
        self.assertEqual(r.b, None)
        self.assertEqual(r.c, 'hello')

    def test_none_value(self):
        """
        The objective of the test is to check that if no value is assigned to the attribute, the validation is returning
        the expect default value as defined in the Field class
        """
        class Example(Record):
            a = Null()
            b = Boolean()
            c = Integer()
            d = Long()
            e = Float()
            f = Double()
            g = Bytes()
            h = String()

        r = Example()

        self.assertIsNone(r.a)
        self.assertFalse(r.b)
        self.assertIsNone(r.c)
        self.assertIsNone(r.d)
        self.assertIsNone(r.e)
        self.assertIsNone(r.f)
        self.assertIsNone(r.g)
        self.assertIsNone(r.h)
    ####

    def test_json_schema(self):

        class Example(Record):
            a = Integer()
            b = Integer()

        # Incompatible variation of the class
        class BadExample(Record):
            a = String()
            b = Integer()

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        'my-json-python-topic',
                        schema=JsonSchema(Example))

        # Validate that incompatible schema is rejected
        try:
            client.subscribe('my-json-python-topic', 'sub-1',
                             schema=JsonSchema(BadExample))
            self.fail('Should have failed')
        except Exception as e:
            pass  # Expected

        try:
            client.subscribe('my-json-python-topic', 'sub-1',
                             schema=StringSchema(BadExample))
            self.fail('Should have failed')
        except Exception as e:
            pass  # Expected

        try:
            client.subscribe('my-json-python-topic', 'sub-1',
                             schema=AvroSchema(BadExample))
            self.fail('Should have failed')
        except Exception as e:
            pass  # Expected

        consumer = client.subscribe('my-json-python-topic', 'sub-1',
                                    schema=JsonSchema(Example))

        r = Example(a=1, b=2)
        producer.send(r)

        msg = consumer.receive()

        self.assertEqual(r, msg.value())

        producer.close()
        consumer.close()
        client.close()

    def test_string_schema(self):
        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        'my-string-python-topic',
                        schema=StringSchema())


        # Validate that incompatible schema is rejected
        try:
            class Example(Record):
                a = Integer()
                b = Integer()

            client.create_producer('my-string-python-topic',
                             schema=JsonSchema(Example))
            self.fail('Should have failed')
        except Exception as e:
            pass  # Expected

        consumer = client.subscribe('my-string-python-topic', 'sub-1',
                                    schema=StringSchema())

        producer.send("Hello")

        msg = consumer.receive()

        self.assertEqual("Hello", msg.value())
        self.assertEqual(b"Hello", msg.data())
        client.close()

    def test_bytes_schema(self):
        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        'my-bytes-python-topic',
                        schema=BytesSchema())

        # Validate that incompatible schema is rejected
        try:
            class Example(Record):
                a = Integer()
                b = Integer()

            client.create_producer('my-bytes-python-topic',
                             schema=JsonSchema(Example))
            self.fail('Should have failed')
        except Exception as e:
            pass  # Expected

        consumer = client.subscribe('my-bytes-python-topic', 'sub-1',
                                    schema=BytesSchema())

        producer.send(b"Hello")

        msg = consumer.receive()

        self.assertEqual(b"Hello", msg.value())
        client.close()

    def test_avro_schema(self):

        class Example(Record):
            a = Integer()
            b = Integer()

        # Incompatible variation of the class
        class BadExample(Record):
            a = String()
            b = Integer()

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        'my-avro-python-topic',
                        schema=AvroSchema(Example))

        # Validate that incompatible schema is rejected
        try:
            client.subscribe('my-avro-python-topic', 'sub-1',
                             schema=AvroSchema(BadExample))
            self.fail('Should have failed')
        except Exception as e:
            pass  # Expected

        try:
            client.subscribe('my-avro-python-topic', 'sub-2',
                             schema=JsonSchema(Example))
            self.fail('Should have failed')
        except Exception as e:
            pass  # Expected

        consumer = client.subscribe('my-avro-python-topic', 'sub-3',
                                    schema=AvroSchema(Example))

        r = Example(a=1, b=2)
        producer.send(r)

        msg = consumer.receive()

        self.assertEqual(r, msg.value())

        producer.close()
        consumer.close()
        client.close()

    def test_json_enum(self):
        class MyEnum(Enum):
            A = 1
            B = 2
            C = 3

        class Example(Record):
            name = String()
            v = MyEnum
            w = CustomEnum(MyEnum)
            x = CustomEnum(MyEnum, required=True, default=MyEnum.A, required_default=True)

        topic = 'my-json-enum-topic'

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        topic=topic,
                        schema=JsonSchema(Example))

        consumer = client.subscribe(topic, 'test',
                                    schema=JsonSchema(Example))

        r = Example(name='test', v=MyEnum.C, w=MyEnum.B)
        producer.send(r)

        msg = consumer.receive()

        self.assertEqual('test', msg.value().name)
        self.assertEqual(MyEnum.C, MyEnum(msg.value().v))
        self.assertEqual(MyEnum.B, MyEnum(msg.value().w))
        self.assertEqual(MyEnum.A, MyEnum(msg.value().x))
        client.close()

    def test_avro_enum(self):
        class MyEnum(Enum):
            A = 1
            B = 2
            C = 3

        class Example(Record):
            name = String()
            v = MyEnum
            w = CustomEnum(MyEnum)
            x = CustomEnum(MyEnum, required=True, default=MyEnum.B, required_default=True)

        topic = 'my-avro-enum-topic'

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        topic=topic,
                        schema=AvroSchema(Example))

        consumer = client.subscribe(topic, 'test',
                                    schema=AvroSchema(Example))

        r = Example(name='test', v=MyEnum.C, w=MyEnum.A)
        producer.send(r)

        msg = consumer.receive()
        msg.value()
        self.assertEqual(MyEnum.C, msg.value().v)
        self.assertEqual(MyEnum.A, MyEnum(msg.value().w))
        self.assertEqual(MyEnum.B, MyEnum(msg.value().x))
        client.close()

    def test_avro_map_array(self):
        class MapArray(Record):
            values = Map(Array(Integer()))

        class MapMap(Record):
            values = Map(Map(Integer()))

        class ArrayMap(Record):
            values = Array(Map(Integer()))

        class ArrayArray(Record):
            values = Array(Array(Integer()))

        topic_prefix = "my-avro-map-array-topic-"
        data_list = (
            (topic_prefix + "0", AvroSchema(MapArray),
                MapArray(values={"A": [1, 2], "B": [3]})),
            (topic_prefix + "1", AvroSchema(MapMap),
                MapMap(values={"A": {"B": 2},})),
            (topic_prefix + "2", AvroSchema(ArrayMap),
                ArrayMap(values=[{"A": 1}, {"B": 2}, {"C": 3}])),
            (topic_prefix + "3", AvroSchema(ArrayArray),
                ArrayArray(values=[[1, 2, 3], [4]])),
        )

        client = pulsar.Client(self.serviceUrl)
        for data in data_list:
            topic = data[0]
            schema = data[1]
            record = data[2]

            producer = client.create_producer(topic, schema=schema)
            consumer = client.subscribe(topic, 'sub', schema=schema)

            producer.send(record)
            msg = consumer.receive()
            self.assertEqual(msg.value().values, record.values)
            consumer.acknowledge(msg)
            consumer.close()
            producer.close()

        client.close()

    def test_avro_required_default(self):
        class MySubRecord(Record):
            _sorted_fields = True
            x = Integer()
            y = Long()
            z = String()

        class Example(Record):
            a = Integer()
            b = Boolean(required=True)
            c = Long()
            d = Float()
            e = Double()
            f = String()
            g = Bytes()
            h = Array(String())
            i = Map(String())
            j = MySubRecord()


        class ExampleRequiredDefault(Record):
            _sorted_fields = True
            a = Integer(required_default=True)
            b = Boolean(required=True, required_default=True)
            c = Long(required_default=True)
            d = Float(required_default=True)
            e = Double(required_default=True)
            f = String(required_default=True)
            g = Bytes(required_default=True)
            h = Array(String(), required_default=True)
            i = Map(String(), required_default=True)
            j = MySubRecord(required_default=True)
        self.assertEqual(ExampleRequiredDefault.schema(), {
                "name": "ExampleRequiredDefault",
                "type": "record",
                "fields": [
                    {
                        "name": "a",
                        "type": [
                            "null",
                            "int"
                        ],
                        "default": None
                    },
                    {
                        "name": "b",
                        "type": "boolean",
                        "default": False
                    },
                    {
                        "name": "c",
                        "type": [
                            "null",
                            "long"
                        ],
                        "default": None
                    },
                    {
                        "name": "d",
                        "type": [
                            "null",
                            "float"
                        ],
                        "default": None
                    },
                    {
                        "name": "e",
                        "type": [
                            "null",
                            "double"
                        ],
                        "default": None
                    },
                    {
                        "name": "f",
                        "type": [
                            "null",
                            "string"
                        ],
                        "default": None
                    },
                    {
                        "name": "g",
                        "type": [
                            "null",
                            "bytes"
                        ],
                        "default": None
                    },
                    {
                        "name": "h",
                        "type": [
                            "null",
                            {
                                "type": "array",
                                "items": "string"
                            }
                        ],
                        "default": None
                    },
                    {
                        "name": "i",
                        "type": [
                            "null",
                            {
                                "type": "map",
                                "values": "string"
                            }
                        ],
                        "default": None
                    },
                    {
                        "name": "j",
                        "type": [
                            "null",
                            {
                                "name": "MySubRecord",
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "x",
                                        "type": [
                                            "null",
                                            "int"
                                        ]
                                    },
                                    {
                                        "name": "y",
                                        "type": [
                                            "null",
                                            "long"
                                        ],
                                    },
                                    {
                                        "name": "z",
                                        "type": [
                                            "null",
                                            "string"
                                        ]
                                    }
                                ]
                            }
                        ],
                        "default": None
                    }
                ]
            })

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
            'my-avro-python-default-topic',
            schema=AvroSchema(Example))

        producer_default = client.create_producer(
            'my-avro-python-default-topic',
            schema=AvroSchema(ExampleRequiredDefault))

        producer.close()
        producer_default.close()

        client.close()

    def test_default_value(self):
        class MyRecord(Record):
            A = Integer()
            B = String()
            C = Boolean(default=True, required=True)
            D = Double(default=6.4)

        topic = "my-default-value-topic"

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                    topic=topic,
                    schema=JsonSchema(MyRecord))

        consumer = client.subscribe(topic, 'test', schema=JsonSchema(MyRecord))

        r = MyRecord(A=5, B="text")
        producer.send(r)

        msg = consumer.receive()
        self.assertEqual(msg.value().A, 5)
        self.assertEqual(msg.value().B, u'text')
        self.assertEqual(msg.value().C, True)
        self.assertEqual(msg.value().D, 6.4)

        producer.close()
        consumer.close()
        client.close()

    def test_serialize_schema_complex(self):
        class Color(Enum):
            red = 1
            green = 2
            blue = 3

        class NestedObj1(Record):
            _sorted_fields = True
            na1 = String()
            nb1 = Double()

        class NestedObj2(Record):
            _sorted_fields = True
            na2 = Integer()
            nb2 = Boolean()
            nc2 = NestedObj1()

        class NestedObj3(Record):
            _sorted_fields = True
            color = CustomEnum(Color)
            na3 = Integer()

        class NestedObj4(Record):
            _avro_namespace = 'xxx4'
            _sorted_fields = True
            na4 = String()
            nb4 = Integer()

        class ComplexRecord(Record):
            _avro_namespace = 'xxx.xxx'
            _sorted_fields = True
            a = Integer()
            b = Integer()
            color = Color
            color2 = Color
            color3 = CustomEnum(Color, required=True, default=Color.red, required_default=True)
            nested = NestedObj2()
            nested2 = NestedObj2()
            mapNested = Map(NestedObj3())
            mapNested2 = Map(NestedObj3())
            arrayNested = Array(NestedObj4())
            arrayNested2 = Array(NestedObj4())

        print('complex schema: ', ComplexRecord.schema())
        self.assertEqual(ComplexRecord.schema(), {
            "name": "ComplexRecord",
            "namespace": "xxx.xxx",
            "type": "record",
            "fields": [
                {"name": "a", "type": ["null", "int"]},
                {'name': 'arrayNested', 'type': ['null', {'type': 'array', 'items':
                    {'name': 'NestedObj4', 'namespace': 'xxx4', 'type': 'record', 'fields': [
                        {'name': 'na4', 'type': ['null', 'string']},
                        {'name': 'nb4', 'type': ['null', 'int']}
                    ]}}
                ]},
                {'name': 'arrayNested2', 'type': ['null', {'type': 'array', 'items': 'xxx4.NestedObj4'}]},
                {"name": "b", "type": ["null", "int"]},
                {'name': 'color', 'type': ['null', {'type': 'enum', 'name': 'Color', 'symbols': [
                    'red', 'green', 'blue']}]},
                {'name': 'color2', 'type': ['null', 'Color']},
                {'name': 'color3', 'default': 'red', 'type': 'Color'},
                {'name': 'mapNested', 'type': ['null', {'type': 'map', 'values':
                    {'name': 'NestedObj3', 'type': 'record', 'fields': [
                        {'name': 'color', 'type': ['null', 'Color']},
                        {'name': 'na3', 'type': ['null', 'int']}
                    ]}}
                ]},
                {'name': 'mapNested2', 'type': ['null', {'type': 'map', 'values': 'NestedObj3'}]},
                {"name": "nested", "type": ['null', {'name': 'NestedObj2', 'type': 'record', 'fields': [
                    {'name': 'na2', 'type': ['null', 'int']},
                    {'name': 'nb2', 'type': ['null', 'boolean']},
                    {'name': 'nc2', 'type': ['null', {'name': 'NestedObj1', 'type': 'record', 'fields': [
                        {'name': 'na1', 'type': ['null', 'string']},
                        {'name': 'nb1', 'type': ['null', 'double']}
                    ]}]}
                ]}]},
                {"name": "nested2", "type": ['null', 'NestedObj2']}
            ]
        })

        def encode_and_decode(schema_type):
            data_schema = AvroSchema(ComplexRecord)
            if schema_type == 'json':
                data_schema = JsonSchema(ComplexRecord)

            nested_obj1 = NestedObj1(na1='na1 value', nb1=20.5)
            nested_obj2 = NestedObj2(na2=22, nb2=True, nc2=nested_obj1)
            r = ComplexRecord(a=1, b=2, color=Color.red, color2=Color.blue,
                              nested=nested_obj2, nested2=nested_obj2,
            mapNested={
                'a': NestedObj3(na3=1, color=Color.green),
                'b': NestedObj3(na3=2),
                'c': NestedObj3(na3=3, color=Color.red)
            }, mapNested2={
                'd': NestedObj3(na3=4, color=Color.red),
                'e': NestedObj3(na3=5, color=Color.blue),
                'f': NestedObj3(na3=6)
            }, arrayNested=[
                NestedObj4(na4='value na4 1', nb4=100),
                NestedObj4(na4='value na4 2', nb4=200)
            ], arrayNested2=[
                NestedObj4(na4='value na4 3', nb4=300),
                NestedObj4(na4='value na4 4', nb4=400)
            ])
            data_encode = data_schema.encode(r)

            data_decode = data_schema.decode(data_encode)
            self.assertEqual(data_decode.__class__.__name__, 'ComplexRecord')
            self.assertEqual(data_decode, r)
            self.assertEqual(r.color3, Color.red)
            self.assertEqual(r.mapNested['a'].color, Color.green)
            self.assertEqual(r.mapNested['b'].color, None)
            print('Encode and decode complex schema finish. schema_type: ', schema_type)

        encode_and_decode('avro')
        encode_and_decode('json')

    def test_sub_record_set_to_none(self):
        class NestedObj1(Record):
            na1 = String()
            nb1 = Double()

        class NestedObj2(Record):
            na2 = Integer()
            nb2 = Boolean()
            nc2 = NestedObj1()

        data_schema = AvroSchema(NestedObj2)
        r = NestedObj2(na2=1, nb2=True)

        data_encode = data_schema.encode(r)
        data_decode = data_schema.decode(data_encode)

        self.assertEqual(data_decode.__class__.__name__, 'NestedObj2')
        self.assertEqual(data_decode, r)
        self.assertEqual(data_decode.na2, 1)
        self.assertTrue(data_decode.nb2)

    def test_produce_and_consume_complex_schema_data(self):
        class Color(Enum):
            red = 1
            green = 2
            blue = 3

        class NestedObj1(Record):
            na1 = String()
            nb1 = Double()

        class NestedObj2(Record):
            na2 = Integer()
            nb2 = Boolean()
            nc2 = NestedObj1()

        class NestedObj3(Record):
            na3 = Integer()
            color = CustomEnum(Color, required=True, required_default=True, default=Color.blue)

        class NestedObj4(Record):
            na4 = String()
            nb4 = Integer()

        class ComplexRecord(Record):
            a = Integer()
            b = Integer()
            color = CustomEnum(Color)
            nested = NestedObj2()
            mapNested = Map(NestedObj3())
            arrayNested = Array(NestedObj4())

        client = pulsar.Client(self.serviceUrl)

        def produce_consume_test(schema_type):
            topic = "my-complex-schema-topic-" + schema_type

            data_schema = AvroSchema(ComplexRecord)
            if schema_type == 'json':
                data_schema= JsonSchema(ComplexRecord)

            producer = client.create_producer(
                        topic=topic,
                        schema=data_schema)

            consumer = client.subscribe(topic, 'test', schema=data_schema)

            nested_obj1 = NestedObj1(na1='na1 value', nb1=20.5)
            nested_obj2 = NestedObj2(na2=22, nb2=True, nc2=nested_obj1)
            r = ComplexRecord(a=1, b=2, nested=nested_obj2, mapNested={
                'a': NestedObj3(na3=1, color=Color.red),
                'b': NestedObj3(na3=2, color=Color.green),
                'c': NestedObj3(na3=3)
            }, arrayNested=[
                NestedObj4(na4='value na4 1', nb4=100),
                NestedObj4(na4='value na4 2', nb4=200)
            ])
            producer.send(r)

            msg = consumer.receive()
            value = msg.value()
            self.assertEqual(value.__class__.__name__, 'ComplexRecord')
            self.assertEqual(value, r)

            print('Produce and consume complex schema data finish. schema_type', schema_type)

        produce_consume_test('avro')
        produce_consume_test('json')

        client.close()

    def custom_schema_test(self):

        def encode_and_decode(schema_definition):
            avro_schema = AvroSchema(None, schema_definition=schema_definition)

            company = {
                "name": "company-name",
                "address": 'xxx road xxx street',
                "employees": [
                    {"name": "user1", "age": 25},
                    {"name": "user2", "age": 30},
                    {"name": "user3", "age": 35},
                ],
                "labels": {
                    "industry": "software",
                    "scale": ">100",
                    "funds": "1000000.0"
                },
                "companyType": "companyType1"
            }
            data = avro_schema.encode(company)
            company_decode = avro_schema.decode(data)
            self.assertEqual(company, company_decode)

        schema_definition = {
            'doc': 'this is doc',
            'namespace': 'example.avro',
            'type': 'record',
            'name': 'Company',
            'fields': [
                {'name': 'name', 'type': ['null', 'string']},
                {'name': 'address', 'type': ['null', 'string']},
                {'name': 'employees', 'type': ['null', {'type': 'array', 'items': {
                    'type': 'record',
                    'name': 'Employee',
                    'fields': [
                        {'name': 'name', 'type': ['null', 'string']},
                        {'name': 'age', 'type': ['null', 'int']}
                    ]
                }}]},
                {'name': 'labels', 'type': ['null', {'type': 'map', 'values': 'string'}]},
                {'name': 'companyType', 'type': ['null', {'type': 'enum', 'name': 'CompanyType', 'symbols':
                    ['companyType1', 'companyType2', 'companyType3']}]}
            ]
        }
        encode_and_decode(schema_definition)
        # Users could load schema from file by `fastavro.schema`
        # Or use `avro.schema` like this `avro.schema.parse(open("examples/company.avsc", "rb").read()).to_json()`
        encode_and_decode(load_schema("examples/company.avsc"))

    def custom_schema_produce_and_consume_test(self):
        client = pulsar.Client(self.serviceUrl)

        def produce_and_consume(topic, schema_definition):
            print('custom schema produce and consume test topic - ', topic)
            example_avro_schema = AvroSchema(None, schema_definition=schema_definition)

            producer = client.create_producer(
                topic=topic,
                schema=example_avro_schema)
            consumer = client.subscribe(topic, 'test', schema=example_avro_schema)

            for i in range(0, 10):
                company = {
                    "name": "company-name" + str(i),
                    "address": 'xxx road xxx street ' + str(i),
                    "employees": [
                        {"name": "user" + str(i), "age": 20 + i},
                        {"name": "user" + str(i), "age": 30 + i},
                        {"name": "user" + str(i), "age": 35 + i},
                    ],
                    "labels": {
                        "industry": "software" + str(i),
                        "scale": ">100",
                        "funds": "1000000.0"
                    },
                    "companyType": "companyType" + str((i % 3) + 1)
                }
                producer.send(company)

            for i in range(0, 10):
                msg = consumer.receive()
                company = {
                    "name": "company-name" + str(i),
                    "address": 'xxx road xxx street ' + str(i),
                    "employees": [
                        {"name": "user" + str(i), "age": 20 + i},
                        {"name": "user" + str(i), "age": 30 + i},
                        {"name": "user" + str(i), "age": 35 + i},
                    ],
                    "labels": {
                        "industry": "software" + str(i),
                        "scale": ">100",
                        "funds": "1000000.0"
                    }
                }
                self.assertEqual(msg.value(), company)
                consumer.acknowledge(msg)

            consumer.close()
            producer.close()

        schema_definition = {
            'doc': 'this is doc',
            'namespace': 'example.avro',
            'type': 'record',
            'name': 'Company',
            'fields': [
                {'name': 'name', 'type': ['null', 'string']},
                {'name': 'address', 'type': ['null', 'string']},
                {'name': 'employees', 'type': ['null', {'type': 'array', 'items': {
                    'type': 'record',
                    'name': 'Employee',
                    'fields': [
                        {'name': 'name', 'type': ['null', 'string']},
                        {'name': 'age', 'type': ['null', 'int']}
                    ]
                }}]},
                {'name': 'labels', 'type': ['null', {'type': 'map', 'values': 'string'}]}
            ]
        }
        produce_and_consume('custom-schema-test-1', schema_definition=schema_definition)
        produce_and_consume('custom-schema-test-2', schema_definition=load_schema("examples/company.avsc"))

        client.close()

if __name__ == '__main__':
    main()
