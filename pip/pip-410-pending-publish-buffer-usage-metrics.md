# PIP-410: Introduce pending publish buffer usage metrics

# Background knowledge

Since https://github.com/apache/pulsar/pull/6178, we introduced broker side backpressure to prevent the broker from 
being overwhelmed by the producers. 

But we don't have a way to monitor the pending publish buffer usage. This PIP proposes to introduce 2 new metrics to
complete the monitoring of the pending publish buffer usage.

# Motivation

Add metrics to monitor the pending publish buffer usage.

# Goals

## In Scope

Broker level:
- Introduce a new metric `pulsar.broker.publish.buffer.usage` to monitor the current pending publish buffer usage.
- Introduce a new metric `pulsar.broker.publish.buffer.max.usage` to monitor the max pending publish buffer usage.

## Out of Scope

NONE

### Metrics
1. `pulsar.broker.publish.buffer.usage`
   - Description: The current pending publish buffer usage.
   - Type: Gauge
   - Attributes:
     - cluster
   - Unit: Bytes
2. `pulsar.broker.publish.buffer.max.usage`
    - Description: The max pending publish buffer usage since the broker started.
    - Type: Gauge
    - Attributes:
        - cluster
    - Unit: Bytes

# Backward & Forward Compatibility
Full backward compatibility is provided.

# Links

* Mailing List discussion thread:
* Mailing List voting thread:
