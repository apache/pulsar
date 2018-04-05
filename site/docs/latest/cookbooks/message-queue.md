---
title: Using Pulsar as a message queue
lead: Although Pulsar is typically known as a real-time messaging system, it's also an excellent choice for a queuing system
---

Message queues are essential components of many large-scale data architectures. If every single work object that passes through your system absolutely *must* be processed in spite of the slowness or downright failure of this or that system component, there's a good chance that you'll need a message queue to step in and ensure that unprocessed data is retained---with correct ordering---until the required actions are taken.

Pulsar is a great choice for a message queue because it was built with [persistent message storage](../../getting-started/ConceptsAndArchitecture#persistent-storage). You can use the same Pulsar installation to act as a real-time message bus and as a message queue if you wish (or just one or the other).

## Client configuration changes

On the client side

```java

```