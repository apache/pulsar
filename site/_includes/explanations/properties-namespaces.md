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
