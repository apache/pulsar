---
title: Apache Pulsar downloads
layout: content
---

Download Pulsar from the [releases page](https://github.com/apache/incubator-pulsar/releases) on GitHub or here:

### Version {{ site.current_version }}

| Type   | Link                                                                                                                                           |
|:-------|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| Source | [apache-pulsar-{{ site.current_version }}-src.tar.gz](http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-src.tar.gz) |
| Binary | [apache-pulsar-{{ site.current_version }}-bin.tar.gz](http://www.apache.org/dyn/closer.cgi/incubator/pulsar/pulsar-{{ site.current_version }}/apache-pulsar-{{ site.current_version }}-bin.tar.gz) |


### Release notes

[https://github.com/apache/incubator-pulsar/releases/tag/v{{site.current_version}}](https://github.com/apache/incubator-pulsar/releases/tag/v{{site.current_version}})

### Getting started

Once you've downloaded a Pulsar release, instructions on getting up and running with a {% popover standalone %} cluster that you can run your laptop can be found in [Run Pulsar locally](/docs/latest/getting-started/LocalCluster).

If you need to connect to an existing Pulsar {% popover cluster %} or {% popover instance %} using an officially supported client, see client docs for these languages:

Client guide | API docs
:------------|:--------
[The Pulsar Java client](../docs/latest/clients/Java) | [Java client Javadoc](../api/client)<br />[Java admin interface Javadoc](../api/admin)
[The Pulsar Python client](../docs/latest/clients/Python) | [pdoc](../api/python)
[The Pulsar C++ client](../docs/latest/clients/Cpp) | [Doxygen docs](../api/cpp)


{% if site.archived_releases %}

### Other releases

| Release   | Download | Release notes                                                                                      |
|:-------|:--------------------------------------------|--------------------------------------------|
{% for version in site.archived_releases
%} {{version}} | [http://archive.apache.org/dist/incubator/pulsar/pulsar-{{version}}](http://archive.apache.org/dist/incubator/pulsar/pulsar-{{version}}) | [Release notes v{{version}}](https://github.com/apache/incubator-pulsar/releases/tag/v{{ version }})|
{% endfor %}

{% endif %}
