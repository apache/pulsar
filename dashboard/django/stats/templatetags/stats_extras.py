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


from django import template
from django.utils import formats
from django.contrib.humanize.templatetags.humanize import intcomma

register = template.Library()

KB = 1 << 10
MB = 1 << 20
GB = 1 << 30
TB = 1 << 40
PB = 1 << 50

def fmt(x):
     return str(formats.number_format(round(x, 1), 1))

@register.filter(name='file_size_value')
def file_size_value(bytes_):
    bytes_ = float(bytes_)
    if bytes_ < KB:   return str(bytes_)
    elif bytes_ < MB: return fmt(bytes_ / KB)
    elif bytes_ < GB: return fmt(bytes_ / MB)
    elif bytes_ < TB: return fmt(bytes_ / GB)
    elif bytes_ < PB: return fmt(bytes_ / TB)
    else:  return fmt(bytes_ / PB)

@register.filter(name='file_size_unit')
def file_size_unit(bytes_):
    if   bytes_ < KB: return 'bytes'
    elif bytes_ < MB: return 'KB'
    elif bytes_ < GB: return 'MB'
    elif bytes_ < TB: return 'GB'
    elif bytes_ < PB: return 'TB'
    else:            return 'PB'


@register.filter(name='mbps')
def mbps(bytes_per_seconds):
    if not bytes_per_seconds: return 0.0
    else: return float(bytes_per_seconds) * 8 / 1024 / 1024

@register.filter(name='safe_intcomma')
def safe_intcomma(n):
    if not n: return 0
    else: return intcomma(n)

@register.filter(name='times')
def times(number):
    return range(1, number + 1)