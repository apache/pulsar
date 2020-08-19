---
id: version-2.6.2-develop-load-manager
title: Modular load manager
sidebar_label: Modular load manager
original_id: develop-load-manager
---

The *modular load manager*, implemented in  [`ModularLoadManagerImpl`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/impl/ModularLoadManagerImpl.java), is a flexible alternative to the previously implemented load manager, [`SimpleLoadManagerImpl`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/impl/SimpleLoadManagerImpl.java), which attempts to simplify how load is managed while also providing abstractions so that complex load management strategies may be implemented.

## Usage

There are two ways that you can enable the modular load manager:

1. Change the value of the `loadManagerClassName` parameter in `conf/broker.conf` from `org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl` to `org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl`.
2. Using the `pulsar-admin` tool. Here's an example:

   ```shell
   $ pulsar-admin update-dynamic-config \
     --config loadManagerClassName \
     --value org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl
   ```

   You can use the same method to change back to the original value. In either case, any mistake in specifying the load manager will cause Pulsar to default to `SimpleLoadManagerImpl`.

## Verification

There are a few different ways to determine which load manager is being used:

1. Use `pulsar-admin` to examine the `loadManagerClassName` element:

    ```shell
   $ bin/pulsar-admin brokers get-all-dynamic-config
   {
     "loadManagerClassName" : "org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl"
   }
   ```

   If there is no `loadManagerClassName` element, then the default load manager is used.

2. Consult a ZooKeeper load report. With the module load manager, the load report in `/loadbalance/brokers/...` will have many differences. for example the `systemResourceUsage` sub-elements (`bandwidthIn`, `bandwidthOut`, etc.) are now all at the top level. Here is an example load report from the module load manager:

    ```json
    {
      "bandwidthIn": {
        "limit": 10240000.0,
        "usage": 4.256510416666667
      },
      "bandwidthOut": {
        "limit": 10240000.0,
        "usage": 5.287239583333333
      },
      "bundles": [],
      "cpu": {
        "limit": 2400.0,
        "usage": 5.7353247655435915
      },
      "directMemory": {
        "limit": 16384.0,
        "usage": 1.0
      }
    }
    ```

    With the simple load manager, the load report in `/loadbalance/brokers/...` will look like this:

    ```json
    {
      "systemResourceUsage": {
        "bandwidthIn": {
          "limit": 10240000.0,
          "usage": 0.0
        },
        "bandwidthOut": {
          "limit": 10240000.0,
          "usage": 0.0
        },
        "cpu": {
          "limit": 2400.0,
          "usage": 0.0
        },
        "directMemory": {
          "limit": 16384.0,
          "usage": 1.0
        },
        "memory": {
          "limit": 8192.0,
          "usage": 3903.0
        }
      }
    }
    ```

3. The command-line [broker monitor](reference-cli-tools.md#monitor-brokers) will have a different output format depending on which load manager implementation is being used.

    Here is an example from the modular load manager:

    ```
    ===================================================================================================================
    ||SYSTEM         |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||
    ||               |0.00           |48.33          |0.01           |0.00           |0.00           |48.33          ||
    ||COUNT          |TOPIC          |BUNDLE         |PRODUCER       |CONSUMER       |BUNDLE +       |BUNDLE -       ||
    ||               |4              |4              |0              |2              |4              |0              ||
    ||LATEST         |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||
    ||SHORT          |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||
    ||LONG           |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |0.00           |0.00           |0.00           |0.00           |0.00           |0.00           ||
    ===================================================================================================================
    ```

    Here is an example from the simple load manager:

    ```
    ===================================================================================================================
    ||COUNT          |TOPIC          |BUNDLE         |PRODUCER       |CONSUMER       |BUNDLE +       |BUNDLE -       ||
    ||               |4              |4              |0              |2              |0              |0              ||
    ||RAW SYSTEM     |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||
    ||               |0.25           |47.94          |0.01           |0.00           |0.00           |47.94          ||
    ||ALLOC SYSTEM   |CPU %          |MEMORY %       |DIRECT %       |BW IN %        |BW OUT %       |MAX %          ||
    ||               |0.20           |1.89           |               |1.27           |3.21           |3.21           ||
    ||RAW MSG        |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |0.00           |0.00           |0.00           |0.01           |0.01           |0.01           ||
    ||ALLOC MSG      |MSG/S IN       |MSG/S OUT      |TOTAL          |KB/S IN        |KB/S OUT       |TOTAL          ||
    ||               |54.84          |134.48         |189.31         |126.54         |320.96         |447.50         ||
    ===================================================================================================================
    ```

It is important to note that the module load manager is _centralized_, meaning that all requests to assign a bundle---whether it's been seen before or whether this is the first time---only get handled by the _lead_ broker (which can change over time). To determine the current lead broker, examine the `/loadbalance/leader` node in ZooKeeper.

## Implementation

### Data

The data monitored by the modular load manager is contained in the [`LoadData`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/LoadData.java) class.
Here, the available data is subdivided into the bundle data and the broker data.

#### Broker

The broker data is contained in the [`BrokerData`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/BrokerData.java) class. It is further subdivided into two parts,
one being the local data which every broker individually writes to ZooKeeper, and the other being the historical broker
data which is written to ZooKeeper by the leader broker.

##### Local Broker Data
The local broker data is contained in the class [`LocalBrokerData`](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/java/org/apache/pulsar/policies/data/loadbalancer/LocalBrokerData.java) and provides information about the following resources:

* CPU usage
* JVM heap memory usage
* Direct memory usage
* Bandwidth in/out usage
* Most recent total message rate in/out across all bundles
* Total number of topics, bundles, producers, and consumers
* Names of all bundles assigned to this broker
* Most recent changes in bundle assignments for this broker

The local broker data is updated periodically according to the service configuration
"loadBalancerReportUpdateMaxIntervalMinutes". After any broker updates their local broker data, the leader broker will
receive the update immediately via a ZooKeeper watch, where the local data is read from the ZooKeeper node
`/loadbalance/brokers/<broker host/port>`

##### Historical Broker Data

The historical broker data is contained in the [`TimeAverageBrokerData`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/TimeAverageBrokerData.java) class.

In order to reconcile the need to make good decisions in a steady-state scenario and make reactive decisions in a critical scenario, the historical data is split into two parts: the short-term data for reactive decisions, and the long-term data for steady-state decisions. Both time frames maintain the following information:

* Message rate in/out for the entire broker
* Message throughput in/out for the entire broker

Unlike the bundle data, the broker data does not maintain samples for the global broker message rates and throughputs, which is not expected to remain steady as new bundles are removed or added. Instead, this data is aggregated over the short-term and long-term data for the bundles. See the section on bundle data to understand how that data is collected and maintained.

The historical broker data is updated for each broker in memory by the leader broker whenever any broker writes their local data to ZooKeeper. Then, the historical data is written to ZooKeeper by the leader broker periodically according to the configuration `loadBalancerResourceQuotaUpdateIntervalMinutes`.

##### Bundle Data

The bundle data is contained in the [`BundleData`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/BundleData.java). Like the historical broker data, the bundle data is split into a short-term and a long-term time frame. The information maintained in each time frame:

* Message rate in/out for this bundle
* Message Throughput In/Out for this bundle
* Current number of samples for this bundle

The time frames are implemented by maintaining the average of these values over a set, limited number of samples, where
the samples are obtained through the message rate and throughput values in the local data. Thus, if the update interval
for the local data is 2 minutes, the number of short samples is 10 and the number of long samples is 1000, the
short-term data is maintained over a period of `10 samples * 2 minutes / sample = 20 minutes`, while the long-term
data is similarly over a period of 2000 minutes. Whenever there are not enough samples to satisfy a given time frame,
the average is taken only over the existing samples. When no samples are available, default values are assumed until
they are overwritten by the first sample. Currently, the default values are

* Message rate in/out: 50 messages per second both ways
* Message throughput in/out: 50KB per second both ways

The bundle data is updated in memory on the leader broker whenever any broker writes their local data to ZooKeeper.
Then, the bundle data is written to ZooKeeper by the leader broker periodically at the same time as the historical
broker data, according to the configuration `loadBalancerResourceQuotaUpdateIntervalMinutes`.

### Traffic Distribution

The modular load manager uses the abstraction provided by [`ModularLoadManagerStrategy`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/ModularLoadManagerStrategy.java) to make decisions about bundle assignment. The strategy makes a decision by considering the service configuration, the entire load data, and the bundle data for the bundle to be assigned. Currently, the only supported strategy is [`LeastLongTermMessageRate`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance/impl/LeastLongTermMessageRate.java), though soon users will have the ability to inject their own strategies if desired.

#### Least Long Term Message Rate Strategy

As its name suggests, the least long term message rate strategy attempts to distribute bundles across brokers so that
the message rate in the long-term time window for each broker is roughly the same. However, simply balancing load based
on message rate does not handle the issue of asymmetric resource burden per message on each broker. Thus, the system
resource usages, which are CPU, memory, direct memory, bandwidth in, and bandwidth out, are also considered in the
assignment process. This is done by weighting the final message rate according to
`1 / (overload_threshold - max_usage)`, where `overload_threshold` corresponds to the configuration
`loadBalancerBrokerOverloadedThresholdPercentage` and `max_usage` is the maximum proportion among the system resources
that is being utilized by the candidate broker. This multiplier ensures that machines with are being more heavily taxed
by the same message rates will receive less load. In particular, it tries to ensure that if one machine is overloaded,
then all machines are approximately overloaded. In the case in which a broker's max usage exceeds the overload
threshold, that broker is not considered for bundle assignment. If all brokers are overloaded, the bundle is randomly
assigned.

