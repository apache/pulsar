# PIP-16: Pulsar "instance" terminology change

* **Status**: Adopted
* **Authors**: Luc Perkins

## Background

Since the initial open source release of Pulsar, the term “instance” has been used to describe a group of Pulsar clusters that work together as a single unit. For an example of this usage, see the [Deploying a Pulsar instance on bare metal](http://pulsar.incubator.apache.org/docs/latest/deployment/instance/) guide.

The problem that has emerged is that the term “instance” is now being used by the [Pulsar Functions](https://github.com/apache/incubator-pulsar/wiki/PIP-15:-Pulsar-Functions) feature, to refer to a process in which a Pulsar Function runs. This usage is inherited from the [Heron](http://heronstreaming.io/) stream processing engine (a key inspiration for Pulsar Functions). A Heron instance is a “[process that handles a single task of a bolt or spout](https://twitter.github.io/heron/docs/concepts/architecture/#heron-instance).”

The problem with using the same term to refer to two very different things in the same project—and thus in the same documentation and codebase—should be immediately clear.

## Goals

There are two goals here:

1. **Clarity and consistency** — No software project should use the same word for multiple things. To do so produces unnecessary cognitive overhead. Pulsar is already a very complex project with a lot of conceptual surface area. Introducing redundant terminology produces unnecessary technical, organizational, and communicative debt and should be avoided at all costs.
2. **More intuitive usage** — Using the term instance to describe a group of things (in Pulsar’s case a group of clusters) doesn’t mesh well with standard usage. An instance is typically an isolated, single thing, not a group, as well as an occurrence of some category or type of thing (as in instantiation). The term instance makes sense for Pulsar Functions as there can be multiple instances of a single Pulsar Function.

## Proposed change

It’s not immediately clear what term should be used for a group of clusters that form a unity, but I would like to propose some candidates here:

Term | Pros | Cons
:----|:-----|:----
Federation | Echoes Star Trek and the United Federation of Planets (consistent with the space theme of Pulsar) and entails cross-cluster unity. | Doesn’t have a direct analogue in any other large-scale software system.
Installation | Straightforward and readily understood. | Ambiguous: an “installation” of Pulsar could also mean Pulsar running on a laptop in standalone mode or a single cluster, etc.
Ensemble | It’s clear from the term that the clusters are working together and constitute a unity. | Already used in other distributed systems contexts, typically as an intra-cluster concept (e.g. an “ensemble of Cassandra nodes”), including in both ZooKeeper and BookKeeper (upon both of which Pulsar is crucially reliant).

Once a term has been decided on, the change would need to be reflected in all documentation and website materials as well as in the codebase itself (e.g. in the CLI tools, error output, class and method names, etc.).

The good news for a change like this is that virtually no one is currently running full multi-cluster Pulsar instances, which means that disruptive impact on the Pulsar community should be minimal.
