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

import copy
from abc import abstractmethod
from collections import OrderedDict
from enum import Enum, EnumMeta
from six import with_metaclass


def _check_record_or_field(x):
    if (type(x) is type and not issubclass(x, Record)) \
            and not isinstance(x, Field):
        raise Exception('Argument ' + x + ' is not a Record or a Field')


class RecordMeta(type):
    def __new__(metacls, name, parents, dct):
        if name != 'Record':
            # Do not apply this logic to the base class itself
            dct['_fields'] = RecordMeta._get_fields(dct)
            dct['_required'] = False
        return type.__new__(metacls, name, parents, dct)

    @classmethod
    def _get_fields(cls, dct):
        # Build a set of valid fields for this record
        fields = OrderedDict()
        for name, value in dct.items():
            if issubclass(type(value), EnumMeta):
                value = CustomEnum(value)
            elif type(value) == RecordMeta:
                # We expect an instance of a record rather than the class itself
                value = value()

            if isinstance(value, Record) or isinstance(value, Field):
                fields[name] = value
        return fields


class Record(with_metaclass(RecordMeta, object)):

    # This field is used to set namespace for Avro Record schema.
    _avro_namespace = None

    # Generate a schema where fields are sorted alphabetically
    _sorted_fields = False

    def __init__(self, default=None, required_default=False, required=False, *args, **kwargs):
        self._required_default = required_default
        self._default = default
        self._required = required

        for k, value in self._fields.items():
            if k in kwargs:
                if isinstance(value, Record) and isinstance(kwargs[k], dict):
                    # Use dict init Record object
                    copied = copy.copy(value)
                    copied.__init__(**kwargs[k])
                    self.__setattr__(k, copied)
                elif isinstance(value, Array) and isinstance(kwargs[k], list) and len(kwargs[k]) > 0 \
                        and isinstance(value.array_type, Record) and isinstance(kwargs[k][0], dict):
                    arr = []
                    for item in kwargs[k]:
                        copied = copy.copy(value.array_type)
                        copied.__init__(**item)
                        arr.append(copied)
                    self.__setattr__(k, arr)
                elif isinstance(value, Map) and isinstance(kwargs[k], dict) and len(kwargs[k]) > 0 \
                    and isinstance(value.value_type, Record) and isinstance(list(kwargs[k].values())[0], dict):
                    dic = {}
                    for mapKey, mapValue in kwargs[k].items():
                        copied = copy.copy(value.value_type)
                        copied.__init__(**mapValue)
                        dic[mapKey] = copied
                    self.__setattr__(k, dic)
                else:
                    # Value was overridden at constructor
                    self.__setattr__(k, kwargs[k])
            elif isinstance(value, Record):
                # Value is a subrecord
                self.__setattr__(k, value)
            else:
                # Set field to default value, without revalidating the default value type
                super(Record, self).__setattr__(k, value.default())

    @classmethod
    def schema(cls):
        return cls.schema_info(set())

    @classmethod
    def schema_info(cls, defined_names):
        namespace_prefix = ''
        if cls._avro_namespace is not None:
            namespace_prefix = cls._avro_namespace + '.'
        namespace_name = namespace_prefix + cls.__name__

        if namespace_name in defined_names:
            return namespace_name

        defined_names.add(namespace_name)

        schema = {
            'type': 'record',
            'name': str(cls.__name__)
        }
        if cls._avro_namespace is not None:
            schema['namespace'] = cls._avro_namespace
        schema['fields'] = []

        def get_filed_default_value(value):
            if isinstance(value, Enum):
                return value.name
            else:
                return value

        if cls._sorted_fields:
            fields = sorted(cls._fields.keys())
        else:
            fields = cls._fields.keys()
        for name in fields:
            field = cls._fields[name]
            field_type = field.schema_info(defined_names) \
                if field._required else ['null', field.schema_info(defined_names)]
            schema['fields'].append({
                'name': name,
                'default': get_filed_default_value(field.default()),
                'type': field_type
            }) if field.required_default() else schema['fields'].append({
                'name': name,
                'type': field_type,
            })

        return schema

    def __setattr__(self, key, value):
        if key == '_default':
            super(Record, self).__setattr__(key, value)
        elif key == '_required_default':
            super(Record, self).__setattr__(key, value)
        elif key == '_required':
            super(Record, self).__setattr__(key, value)
        else:
            if key not in self._fields:
                raise AttributeError('Cannot set undeclared field ' + key + ' on record')

            # Check that type of value matches the field type
            field = self._fields[key]
            value = field.validate_type(key, value)
            super(Record, self).__setattr__(key, value)

    def __eq__(self, other):
        for field in self._fields:
            if self.__getattribute__(field) != other.__getattribute__(field):
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return str(self.__dict__)

    def type(self):
        return str(self.__class__.__name__)

    def python_type(self):
        return self.__class__

    def validate_type(self, name, val):
        if val is None and not self._required:
            return self.default()

        if not isinstance(val, self.__class__):
            raise TypeError("Invalid type '%s' for sub-record field '%s'. Expected: %s" % (
                type(val), name, self.__class__))
        return val

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None

    def required_default(self):
        return self._required_default


class Field(object):
    def __init__(self, default=None, required=False, required_default=False):
        if default is not None:
            default = self.validate_type('default', default)
        self._default = default
        self._required_default = required_default
        self._required = required

    @abstractmethod
    def type(self):
        pass

    @abstractmethod
    def python_type(self):
        pass

    def validate_type(self, name, val):
        if val is None and not self._required:
            return self.default()

        if type(val) != self.python_type():
            raise TypeError("Invalid type '%s' for field '%s'. Expected: %s" % (type(val), name, self.python_type()))
        return val

    def schema(self):
        # For primitive types, the schema would just be the type itself
        return self.type()

    def schema_info(self, defined_names):
        return self.type()

    def default(self):
        return self._default

    def required_default(self):
        return self._required_default


# All types


class Null(Field):
    def type(self):
        return 'null'

    def python_type(self):
        return type(None)

    def validate_type(self, name, val):
        if val is not None:
            raise TypeError('Field ' + name + ' is set to be None')
        return val


class Boolean(Field):
    def type(self):
        return 'boolean'

    def python_type(self):
        return bool

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return False


class Integer(Field):
    def type(self):
        return 'int'

    def python_type(self):
        return int

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None


class Long(Field):
    def type(self):
        return 'long'

    def python_type(self):
        return int

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None


class Float(Field):
    def type(self):
        return 'float'

    def python_type(self):
        return float

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None


class Double(Field):
    def type(self):
        return 'double'

    def python_type(self):
        return float

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None


class Bytes(Field):
    def type(self):
        return 'bytes'

    def python_type(self):
        return bytes

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None


class String(Field):
    def type(self):
        return 'string'

    def python_type(self):
        return str

    def validate_type(self, name, val):
        t = type(val)

        if val is None and not self._required:
            return self.default()

        if not (t is str or t.__name__ == 'unicode'):
            raise TypeError("Invalid type '%s' for field '%s'. Expected a string" % (t, name))
        return val

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None

# Complex types


class CustomEnum(Field):
    def __init__(self, enum_type, default=None, required=False, required_default=False):
        if not issubclass(enum_type, Enum):
            raise Exception(enum_type + " is not a valid Enum type")
        self.enum_type = enum_type
        self.values = {}
        for x in enum_type.__members__.values():
            self.values[x.value] = x
        super(CustomEnum, self).__init__(default, required, required_default)

    def type(self):
        return 'enum'

    def python_type(self):
        return self.enum_type

    def validate_type(self, name, val):
        if val is None:
            return None

        if type(val) is str:
            # The enum was passed as a string, we need to check it against the possible values
            if val in self.enum_type.__members__:
                return self.enum_type.__members__[val]
            else:
                raise TypeError(
                    "Invalid enum value '%s' for field '%s'. Expected: %s" % (val, name, self.enum_type.__members__.keys()))
        elif type(val) is int:
            # The enum was passed as an int, we need to check it against the possible values
            if val in self.values:
                return self.values[val]
            else:
                raise TypeError(
                    "Invalid enum value '%s' for field '%s'. Expected: %s" % (val, name, self.values.keys()))
        elif type(val) != self.python_type():
            raise TypeError("Invalid type '%s' for field '%s'. Expected: %s" % (type(val), name, self.python_type()))
        else:
            return val

    def schema(self):
        return self.schema_info(set())

    def schema_info(self, defined_names):
        if self.enum_type.__name__ in defined_names:
            return self.enum_type.__name__
        defined_names.add(self.enum_type.__name__)
        return {
            'type': self.type(),
            'name': self.enum_type.__name__,
            'symbols': [x.name for x in self.enum_type]
        }

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None


class Array(Field):
    def __init__(self, array_type, default=None, required=False, required_default=False):
        _check_record_or_field(array_type)
        self.array_type = array_type
        super(Array, self).__init__(default=default, required=required, required_default=required_default)

    def type(self):
        return 'array'

    def python_type(self):
        return list

    def validate_type(self, name, val):
        if val is None:
            return None

        super(Array, self).validate_type(name, val)

        for x in val:
            if type(x) != self.array_type.python_type():
                raise TypeError('Array field ' + name + ' items should all be of type '
                                + self.array_type.python_type())
        return val

    def schema(self):
        return self.schema_info(set())

    def schema_info(self, defined_names):
        return {
            'type': self.type(),
            'items': self.array_type.schema_info(defined_names) if isinstance(self.array_type, (Array, Map, Record))
                else self.array_type.type()
        }

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None


class Map(Field):
    def __init__(self, value_type, default=None, required=False, required_default=False):
        _check_record_or_field(value_type)
        self.value_type = value_type
        super(Map, self).__init__(default=default, required=required, required_default=required_default)

    def type(self):
        return 'map'

    def python_type(self):
        return dict

    def validate_type(self, name, val):
        if val is None:
            return None

        super(Map, self).validate_type(name, val)

        for k, v in val.items():
            if type(k) != str and not is_unicode(k):
                raise TypeError('Map keys for field ' + name + '  should all be strings')
            if type(v) != self.value_type.python_type():
                raise TypeError('Map values for field ' + name + ' should all be of type '
                                + self.value_type.python_type())

        return val

    def schema(self):
        return self.schema_info(set())

    def schema_info(self, defined_names):
        return {
            'type': self.type(),
            'values': self.value_type.schema_info(defined_names) if isinstance(self.value_type, (Array, Map, Record))
                else self.value_type.type()
        }

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return None


# Python3 has no `unicode` type, so here we use a tricky way to check if the type of `x` is `unicode` in Python2
# and also make it work well with Python3.
def is_unicode(x):
    return 'encode' in dir(x) and type(x.encode()) == str
