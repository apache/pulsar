<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

Pulsar was designed from the ground up to be a {% popover multi-tenant %} system. In Pulsar, {% popover tenants %} are identified by [properties](#properties). Properties are the highest administrative unit within a Pulsar {% popover instance %}. Within properties

### Properties

To each property in a Pulsar instance you can assign:

* An [authorization](../../admin/Authz#authorization) scheme
* The set of {% popover clusters %} to which the property applies

### Namespaces

{% popover Properties %} and {% popover namespaces %} are two key concepts of Pulsar to support {% popover multi-tenancy %}.

* A **property** identifies a {% popover tenant %}. Pulsar is provisioned for a specified property with appropriate capacity allocated to the property.
* A **namespace** is the administrative unit nomenclature within a property. The configuration policies set on a namespace apply to all the topics created in such namespace. A property may create multiple namespaces via self-administration using REST API and CLI tools. For instance, a property with different applications can create a separate namespace for each application.

Names for topics in the same namespace will look like this:

{% include topic.html p="my-property" c="us-w" n="my-app1" t="my-topic-1" %}
{% include topic.html p="my-property" c="us-w" n="my-app1" t="my-topic-2" %}
{% include topic.html p="my-property" c="us-w" n="my-app1" t="my-topic-3" %}
