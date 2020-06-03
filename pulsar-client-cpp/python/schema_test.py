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
import pulsar
from pulsar.schema import *
from enum import Enum
import json


class SchemaTest(TestCase):

    serviceUrl = 'pulsar://localhost:6650'

    def test_simple(self):
        class Color(Enum):
            red = 1
            green = 2
            blue = 3

        class Example(Record):
            a = String()
            b = Integer()
            c = Array(String())
            d = Color
            e = Boolean()
            f = Float()
            g = Double()
            h = Bytes()
            i = Map(String())

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
            ]
        })

    def test_complex(self):
        class MySubRecord(Record):
            x = Integer()
            y = Long()
            z = String()

        class Example(Record):
            a = String()
            sub = MySubRecord     # Test with class
            sub2 = MySubRecord()  # Test with instance

        self.assertEqual(Example.schema(), {
            "name": "Example",
            "type": "record",
            "fields": [
                {"name": "a", "type": ["null", "string"]},
                {"name": "sub",
                 "type": ["null", {
                     "name": "MySubRecord",
                     "type": "record",
                     "fields": [{"name": "x", "type": ["null", "int"]},
                                {"name": "y", "type": ["null", "long"]},
                                {"name": "z", "type": ["null", "string"]}]
                 }]
                 },
                 {"name": "sub2",
                  "type": ["null", {
                     "name": "MySubRecord",
                     "type": "record",
                     "fields": [{"name": "x", "type": ["null", "int"]},
                                {"name": "y", "type": ["null", "long"]},
                                {"name": "z", "type": ["null", "string"]}]
                 }]
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

        try:
            Record('xyz', a=1, b=2)
            self.fail('Should have failed')
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
        client.close()

    def test_json_enum(self):
        class MyEnum(Enum):
            A = 1
            B = 2
            C = 3

        class Example(Record):
            name = String()
            v = MyEnum

        topic = 'my-json-enum-topic'

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        topic=topic,
                        schema=JsonSchema(Example))

        consumer = client.subscribe(topic, 'test',
                                    schema=JsonSchema(Example))

        r = Example(name='test', v=MyEnum.C)
        producer.send(r)

        msg = consumer.receive()

        self.assertEqual('test', msg.value().name)
        self.assertEqual(MyEnum.C, MyEnum(msg.value().v))
        client.close()

    def test_avro_enum(self):
        class MyEnum(Enum):
            A = 1
            B = 2
            C = 3

        class Example(Record):
            name = String()
            v = MyEnum

        topic = 'my-avro-enum-topic'

        client = pulsar.Client(self.serviceUrl)
        producer = client.create_producer(
                        topic=topic,
                        schema=AvroSchema(Example))

        consumer = client.subscribe(topic, 'test',
                                    schema=AvroSchema(Example))

        r = Example(name='test', v=MyEnum.C)
        producer.send(r)

        msg = consumer.receive()
        msg.value()
        self.assertEqual(MyEnum.C, msg.value().v)
        client.close()


if __name__ == '__main__':
    main()
