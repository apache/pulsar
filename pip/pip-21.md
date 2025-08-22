# PIP-21: Pulsar Edge Component

- **Status**: Proposed
- **Author**: [David Kjerrumgaard](https://github.com/orgs/streamlio/people/david-streamlio)
- **Pull Request**: -
- **Mailing List discussion**: 

I am proposing the creation of a lightweight, small-resource footprint software component that can be deployed on “edge” devices such as IoT Gateway boxes or similar. Target environment resource specifications are; 1 CPUs, 1 GB RAM, 4GB Flash storage, and a 1 Gbps NIC.

## Motivation

The current Pulsar function runtime environment is located on a Pulsar Broker instance, which is typically deployed in a corporate datacenter or public cloud provider, behind a firewall. This introduces a significant amount of latency from the time an event was generated until the time a Pulsar function is able to act upon it.



![Figure 1: Current State](img/pip21-image1.jpg) 



Therefore, it would be desirable to create a software component that provides an environment for running Pulsar functions in a resource constrained environment. 

In the desired state, we would be able to host simple Pulsar functions on the IoT Gateway device itself, much closer to the event source. This allows us to perform functions such as filtering, alerting, message prioritization, or even probabilistic algorithms with small memory requirements such as Bloom Filtering, Top-K, event frequency, distinct element counts, or anomaly detection based on t-digest.

Events can still be forwarded to the downstream Pulsar cluster for longer-term storage, and additional Pulsar function execution.

![Figure 2: Desired State](img/pip21-image2.jpg) 


## Requirements

The Pulsar Edge Component should have the following capabilities:
- The ability to run in a resource constrained environment.
- A REST interface that allows users to submit function packages (jar files, python files, etc)
- A REST interface that allows users to start/stop/create/configure Pulsar functions 
- Should provide the same interface as the CLI 
  - Support for secure SSL/TLS communication for the REST endpoint
- A runtime environment for Pulsar functions.
- The ability to forward messages to multiple topics on a Pulsar cluster over TCP.
- The ability to batch messages before forwarding to Pulsar (to improve throughput)
- A communication back-channel that allows Pulsar clients to send instructions / messages back to the Edge component, e.g. if an alert condition is detected upstream, we want to be able to send a “turn off the machine” message back to the device and would like to use the edge component as the intermediary for this. (This is a “nice to have”)

## Design

The Pulsar Edge Component will wrap a standard pulsar client object, that it will use to forward the events to the Pulsar Cluster. It will also have the same pub/sub API as the Pulsar Client.  The Pulsar edge component should also provide a mechanism for enabling message batching prior to transmitting them over TCP to the Pulsar cluster. This will provide useful in high-volume environments when we want to improve throughput.

IoT applications would be used to capture the sensor data that is transmitted to the ioT gateway device over short-range protocols such as ZigBee, BlueTooth, convert it into the desired format, and publish it to the Pulsar Gateway object using the pub/sub API. 

![Figure 3: Pulsar Edge Component Design](img/pip21-image3.jpg)
