# PIP-65: Adapting Pulsar IO Sources to support Batch Sources

* **Status**: Proposal
 * **Author**: [Sanjeev Kulkarni](https://github.com/srkukarni), [Jerry Peng](https://github.com/jerrypeng)
 * **Mailing List discussion**: 
 * **Target Release**: 2.6.0

## Background

Currently Pulsar IO provides a very simple SDK and flexible runtimes for sources and sinks. The SDK is oriented towards supporting streaming sources and sinks(like reading from say a AWS Kinesis stream). However numerous sources of data are of non-streaming nature. Consider an example of reading from a file/object service. Files/Objects are added to this service on a regular basis that are needed to be copied to Pulsar. There may be some ways the service can notify when new data is available(for example S3 notification service for new object creation). Other times we might just want to do this on a regular cron like schedule(for example FTP service). In either case there is a (1. discover new data to be ingested and 2. ingest it) cycle initiated by some trigger. 
This document proposes adding explicit sdk support for these batch type sources. We outline the sdk interfaces as well as some implementation directions. 

## Nuances of Batch Sources

The most salient feature of a BatchSource is that data arrival is not continuous but happens in spurts. The data collection cycle of a BatchSource is triggered by an event. These events could be a notification the source system provides when new data arrives. Amazon S3 notifications are an example of this type of sources. Other times the collector needs to poll the source system to see if there is new data. Collection of data from FTP server is an example of these type of sources. Thus one of the most low hanging fruit is abstracting out this trigger mechanism away from the collection itself.

Once a new cycle of data collection begins, the source typically needs to do some kind of discovery. This discovery is primarily to figure out the delta of data, i.e. new data that has arrived since the last discovery and collection cycle. Typically this discovery needs to happen only at one place and not in all source instances. Discovering the delta in all instances every cycle is not only a waste of work, it could also put undue strain on control apis of the source systems.

The discovery process itself could itself take a long time(in relation to the cycle interval time). Having all other instances wait until the discovery process is finished could increase the latency/freshness of the new data being collected. Thus its pretty useful to take these discovered tasks as they are discovered and hand them over to all instances of the source. This requires some sort of orchestration between the discover phase and the actual data collection phase.

In light of the above characteristics of Batch sources, it makes sense to provide framework level support that makes it easy for source writers to write these connectors and have the framework take care of all orchestration between the discover phase and collect phase.

### BatchSource API

We propose adding a new interface called BatchSource for writing batch sources. The actual api reference can be found in the API Reference section below.  The lifecycle of the BatchSource is as follows
1. open - called once when connector is started.  Can use method to perform
          certain one-time operations such as init/setup operations. This is called on all
         instances of the source and is analogous to the open method of the streaming Source api.
2. discover (called only on one instance (Currently instance zero(0), but might change later))
    - The discovery phase will be executed on one instance of the connector.
    - discover is triggered by the BatchSourceTriggerer class configured for this source.
    - As and when discover discovers new tasks, it will emit them using the taskEater method.
    - The framework will distribute the discovered tasks among all instances
3. prepare - is called on an instance when there is a new discovered task assigned for that instance
    - The framework decides which discovered task is routed to which source instance. The connector
      does not currently have a way to influence this.
    - prepare is only called when the instance has fetched all records using readNext for its previously
      assigned discovered task.
4. readNext - called repeatedly by the framework to fetch the next record. If there are no
              more records available to emit, the connector should return null. That indicates
              that all records for that particular discovered task is complete.
5. close - called when the source is stopped/deleted. This is analogous to the streaming Source api.

### BatchSource Triggerer API

We propose adding a new interface called BatchSourceTriggerer for triggering batch source discovery. The actual api reference can be found in the API Reference section below.  These triggerers trigger the discovery method of the batch source that they are attached to. A BatchSource is configured to use a particular BatchSourceTriggerer at the time of job submission. The lifecycle of the object is as follows
1. init - Called after the object is created by reflection.
     - The triggerer is created on only one instance of the source job.
     - The triggerer is passed its configuration in the init method.
     - Trigger also has access to the SourceContext of the BatchSource that it is attached to.
     - It can use this context to get metadata information about the source as well things like secrets
     - This method just inits the triggerer. It doesn't start its execution.
2. start - Is called to actually start the running of the triggerer.
     -  Triggerer will use the 'trigger' ConsumerFunction to actually trigger the discovery process
3. stop - Stop from further triggering discovers

### BatchSourceConfig at submission

The BatchSource sources will be submitted to pulsar function workers using the same SourceConfig api to the same rest end point as the current streaming sources. The SourceConfig will be enhanced by adding a new BatchSourceConfig parameter. The existence of this config parameter indicates to the framework that this is a BatchSource and not the usual Source. This config also ties the BatchSourceTrigger class names that will be used to trigger the discovery process. 

### Executor based implementation

From the Pulsar IO framework, BatchSources will be executed as existing streaming designed Sources. A BatchSourceExecutor Source implementation will wrap the BatchSource and present itself as a Source to the framework. At submission time of the BatchSource, the framework will look for the existence of BatchSourceConfig in the supplied SourceConfig. Its presence will lead to automatic class rewrite to the BatchSourceExecutor. This implementation mirrors the implementation of WindowFunction as regular Functions from the framework's perspective.

### Intermediate topic for discovered task distribution

The BatchSourceExecutor will use an intermediate topic to store the discovered tasks. All the instances will have a shared subscription to this intermediate topic. This will ensure that tasks never get lost and will use built-in Pulsar tools to load balance across the all of the batch source instances.

### Packaging concerns

Just like Sources, BatchSources will be packaged as nars and submitted to the function workers. The only additional thing is to also package the appropriate BatchSourceTriggerer jars in that nar.

## API Reference

#### BatchSource

```java
public interface BatchSource<T> extends AutoCloseable {

    /**
     * Open connector with configuration.
     *
     * @param config config that's supplied for source
     * @param context environment where the source connector is running
     * @throws Exception IO type exceptions when opening a connector
     */
    void open(final Map<String, Object> config, SourceContext context) throws Exception;

    /**
     * Discovery phase of a connector.  This phase will only be run on one instance,
     * i.e. instance 0, of the connector.
     * Implementations use the taskEater consumer to output serialized representation of tasks
     * as they are discovered.
     *
     * @param taskEater function to notify the framework about the new task received.
     * @throws Exception during discover
     */
    void discover(Consumer<byte[]> taskEater) throws Exception;

    /**
     * Called when a new task appears for this connector instance.
     *
     * @param task the serialized representation of the task
     */
    void prepare(byte[] task) throws Exception;

    /**
     * Read data and return a record
     * Return null if no more records are present for this task
     * @return a record
     */
    Record<T> readNext() throws Exception;
}
```

#### BatchSourceConfig

```java
public class BatchSourceConfig {
    public static final String BATCHSOURCE_CONFIG_KEY = "__BATCHSOURCECONFIGS__";
    public static final String BATCHSOURCE_CLASSNAME_KEY = "__BATCHSOURCECLASSNAME__";

    // The class used for triggering the discovery process
    private String discoveryTriggererClassName;
    // The config needed for the discovery Triggerer init
    private Map<String, Object> discoveryTriggererConfig;
}
```

#### BatchSourceTriggerer

```java
public interface BatchSourceTriggerer {

    /**
     * initializes the Triggerer with given config. Note that the triggerer doesn't start running
     * until start is called.
     *
     * @param config config needed for triggerer to run
     * @param sourceContext The source context associated with the source
     * The parameter passed to this trigger function is an optional description of the event that caused the trigger
     * @throws Exception throws any exceptions when initializing
     */
    void init(Map<String, Object> config, SourceContext sourceContext) throws Exception;

    /**
     * Triggerer should actually start looking out for trigger conditions.
     *
     * @param trigger The function to be called when its time to trigger the discover
     *                This function can be passed any metadata about this particular
     *                trigger event as its argument
     * This method should return immediately. It is expected that implementations will use their own mechanisms
     *                to schedule the triggers.
     */
    void start(Consumer<String> trigger);

    /**
     * Triggerer should stop triggering.
     *
     */
    void stop();
}
```
