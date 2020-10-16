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

from abc import abstractmethod, ABCMeta
from enum import Enum, EnumMeta
from collections import OrderedDict
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
                # Wrap Python enums
                value = _Enum(value)
            elif type(value) == RecordMeta:
                # We expect an instance of a record rather than the class itself
                value = value()

            if isinstance(value, Record) or isinstance(value, Field):
                fields[name] = value
        return fields


class Record(with_metaclass(RecordMeta, object)):

    def __init__(self, *args, **kwargs):
        if args:
            # Only allow keyword args
            raise TypeError('Non-keyword arguments not allowed when initializing Records')

        for k, value in self._fields.items():
            if k in kwargs:
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
        schema = {
            'name': str(cls.__name__),
            'type': 'record',
            'fields': []
        }

        for name in sorted(cls._fields.keys()):
            field = cls._fields[name]
            field_type = field.schema() if field._required else ['null', field.schema()]
            schema['fields'].append({
                'name': name,
                'type': field_type
            })
        return schema

    def __setattr__(self, key, value):
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

    def __str__(self):
        return str(self.__dict__)

    def type(self):
        return str(self.__class__.__name__)

    def validate_type(self, name, val):
        if not isinstance(val, self.__class__):
            raise TypeError("Invalid type '%s' for sub-record field '%s'. Expected: %s" % (
                type(val), name, self.__class__))
        return val


class Field(object):
    def __init__(self, default=None, required=False):
        if default is not None:
            default = self.validate_type('default', default)
        self._default = default
        self._required = required

    @abstractmethod
    def type(self):
        pass

    @abstractmethod
    def python_type(self):
        pass

    def validate_type(self, name, val):
        if type(val) != self.python_type():
            raise TypeError("Invalid type '%s' for field '%s'. Expected: %s" % (type(val), name, self.python_type()))
        return val

    def schema(self):
        # For primitive types, the schema would just be the type itself
        return self.type()

    def default(self):
        return self._default

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
            return 0


class Long(Field):
    def type(self):
        return 'long'

    def python_type(self):
        return int

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return 0


class Float(Field):
    def type(self):
        return 'float'

    def python_type(self):
        return float

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return 0.0


class Double(Field):
    def type(self):
        return 'double'

    def python_type(self):
        return float

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return 0.0


class Bytes(Field):
    def type(self):
        return 'bytes'

    def python_type(self):
        return bytes

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return bytes('')


class String(Field):
    def type(self):
        return 'string'

    def python_type(self):
        return str

    def validate_type(self, name, val):
        t = type(val)
        if not (t is str or t.__name__ == 'unicode'):
            raise TypeError("Invalid type '%s' for field '%s'. Expected a string" % (t, name))
        return val

    def default(self):
        if self._default is not None:
            return self._default
        else:
            return str('')

# Complex types


class _Enum(Field):
    def __init__(self, enum_type):
        if not issubclass(enum_type, Enum):
            raise Exception(enum_type + " is not a valid Enum type")
        self.enum_type = enum_type
        self.values = {}
        for x in enum_type.__members__.values():
            self.values[x.value] = x
        super(_Enum, self).__init__()

    def type(self):
        return 'enum'

    def python_type(self):
        return self.enum_type

    def validate_type(self, name, val):
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
        return {
            'type': self.type(),
            'name': self.enum_type.__name__,
            'symbols': [x.name for x in self.enum_type]
        }


class Array(Field):
    def __init__(self, array_type):
        _check_record_or_field(array_type)
        self.array_type = array_type
        super(Array, self).__init__()

    def type(self):
        return 'array'

    def python_type(self):
        return list

    def validate_type(self, name, val):
        super(Array, self).validate_type(name, val)

        for x in val:
            if type(x) != self.array_type.python_type():
                raise TypeError('Array field ' + name + ' items should all be of type '
                                + self.array_type.python_type())
        return val

    def schema(self):
        return {
            'type': self.type(),
            'items': self.array_type.schema() if isinstance(self.array_type, Record) 
                else self.array_type.type()
        }


class Map(Field):
    def __init__(self, value_type):
        _check_record_or_field(value_type)
        self.value_type = value_type
        super(Map, self).__init__()

    def type(self):
        return 'map'

    def python_type(self):
        return dict

    def validate_type(self, name, val):
        super(Map, self).validate_type(name, val)

        for k, v in val.items():
            if type(k) != str:
                raise TypeError('Map keys for field ' + name + '  should all be strings')
            if type(v) != self.value_type.python_type():
                raise TypeError('Map values for field ' + name + ' should all be of type '
                                + self.value_type.python_type())

        return val

    def schema(self):
        return {
            'type': self.type(),
            'values': self.value_type.schema() if isinstance(self.value_type, Record)
                else self.value_type.type()
        }
