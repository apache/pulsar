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

Pulsar was designed from the ground up to be a {% popover multi-tenant %} system. In Pulsar, {% popover tenants %} are the highest administrative unit within a Pulsar {% popover instance %}.

### Tenants

To each property in a Pulsar instance you can assign:

* An [authorization](../../security/authorization) scheme
* The set of {% popover clusters %} to which the tenant's configuration applies

### Namespaces

{% popover Tenants %} and {% popover namespaces %} are two key concepts of Pulsar to support {% popover multi-tenancy %}.

* Pulsar is provisioned for specified {% popover tenants %} with appropriate capacity allocated to the tenant.
* A {% popover namespace %} is the administrative unit nomenclature within a tenant. The configuration policies set on a namespace apply to all the topics created in that namespace. A tenant may create multiple namespaces via self-administration using the REST API and the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool. For instance, a tenant with different applications can create a separate namespace for each application.

Names for topics in the same namespace will look like this:

{% include topic.html ten="my-tenant" n="my-app1" t="my-topic-1" %}
{% include topic.html ten="my-tenant" n="my-app1" t="my-topic-2" %}
{% include topic.html ten="my-tenant" n="my-app1" t="my-topic-3" %}
