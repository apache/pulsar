# PIP-89: Structured document logging

* **Status**: 'Under Discussion'
* **Author**: Ivan Kelly (ivank@apache.org)
* **Pull Request**: -
* **Mailing List discussion**: http://mail-archives.apache.org/mod_mbox/pulsar-dev/202108.mbox/browser
* **Release**: -

# Motivation

As our pulsar clusters have grown we have found it harder and harder to debug production issues. We have found that we have to rely on domain experts and find the causes of issues, and often find that key information is missing to discover the root cause. We often end up having to access zookeeper directly to discover the state. When we have a question like "To which broker was topic X assigned at time Y?", we have to dig into the code to find the exact log messages which give some indication to the answer.

The core of the problem is Pulsar's logging. In general it does not surface enough information about state changes in the system. 

Pulsar's logging makes heavy use of string interpolation. String interpolation makes it cumbersome and error prone to add new fields, and result in log messages which require complex regexes to search.

This situation is unsustainable. The reliance on domain experts is a source of burnout, and the opacity of the system makes it hard to ramp up new people. Pulsar is not that complex conceptually. It should be possible for any engineer with a basic knowledge of the concepts to be able to dig into the logs of a cluster and understand the current state of the system.


# Public Interfaces

No public interface change

# Proposed Changes

## TL;DR
We are proposing to add structured, documented event logging to Pulsar. We will add a wrapper to slf4j to instrument the code. The change will be gradual, and work equally as well with grep as with complex tools like Splunk. The aim is to log all state changes in the control plane.
## Structured, documented event logging
Structured logging means that logged events have context added to them in key value form. This is in contrast to unstructured logging, where events are interpolated into the string in an ad hoc manner. 

For example, for a lookup request the logged event should have the following data added to the log:
* RequestId
* Authoritative-ness of the request
* Topic
* PermitsAvailable
* Duration of processing
* Result (if successful)

By adding each of these as individual fields, we can then query the logs to find all instances where processing took more than 10ms, or all instances where permits were near to zero. These kinds of queries are currently not possible.

Documented logging means that the events themselves are documented. Taking the lookup example again, assuming context is logged structurally, there is little you can add to a one line logging message that gives more information than just "LOOKUP". However, there is likely context information that would be useful for someone less familiar with the system. If we use an enum for the event message, then the event itself can be documented with javadoc as below.

```java
LoggedEvent e = parentEvent.newTimedEvent()
    .attr("authoritative", authoritative)
    .attr("reqId", requestId)
    .attr("topic", topic)
    .attr("permitsAvailable", permitsAvailable);

// DO THE LOOKUP 

e.attr("result", lookupResult)
 .info(Events.LOOKUP);

public enum Events {
    ...
    /**
     * Lookup the location of the broker where a topic
     * is assigned. In the case of partitioned topics,
     * this will return the number of partitions in the
     * topic, which must then be individually looked up.
     * Note that the lookup request for a topic can be
     * served by any broker, not just the broker that
     * owns the topic.
     */
    LOOKUP
}
```

## Logging errors in the data plane
So far the discussion has focussed on the control plane. Logging in generally is quite heavy, so we want to avoid it as much as possible in the data plane. However, there are cases where we want to log in the data plane, namely when an error occurs. 
Not all errors in the data plane are interesting. Pulsar writes asynchronously to bookkeeper, so the common case is that there are multiple requests in flight at any time. If one write request to a ledger throws an error, all subsequent write requests will also throw an error. Only the first error is interesting. To log the rest would create a lot of noise in the logs. For this scenario we'll use sampled logging. If a log is sampled, it will only log once every given time period.
```java
parentEvent.newSampledEvent(1, TimeUnit.SECOND)
    .attr("ledgerId", ledger.getId())
    .attr("metadata", ledger.getLedgerMetadata())
    .exception(ex)
    .warn(Events.LEDGER_WRITE_ERROR)
```

## Passing context down the call stack
It is very useful to be able to tie events pertaining to the same operation together. For example, when a topic is created, there is a ledger creation event, and a managed ledger update event. When the user initiates the topic creation, an ID will be generated which will be threaded through the subsequent events. 

However, we don't want to have to modify all function signatures to pass in this context information. Instead we will take advantage of thread local storage. When the root event is created, it stores its context in a TLS variable. This can then be retrieved further down the call stack.
```java
public void method1() {
    Event parentEvent = logger.newRootEvent(); // generates traceId
    method2();
}

public void method2() {
    logger.fromCallstack()
        .newEvent()
        .info(Events.SOMETHING); // has traceId attached
}
```
## Wrapping Slf4j
This change is necessarily a brownfield development. Not all subcomponents can be changed at once so structured logging will have to coexist with traditional logging for some time. We also want structured logging to work with existing logging configuration. 
For this reason, the backend for structured logging will be slf4j. The actual structured information will be transmitted using MDC, which is available in all the backends supported by slf4j. 

## Proposed API
```java
interface StEvLog {
    Event fromCallstack();
    Event newRootEvent();

    interface Event {
            Event newTimedEvent();
            Event newEvent();
            Event newSampledEvent(int duration, TimeUnit unit);

            Event attr(String key, Object value);
            Event exception(Throwable t);

            void info(Enum<?> eventId);
            void warn(Enum<?> eventId);
            void error(Enum<?> eventId);
    }
}
```
## Example output
### With json appender
```json
{"instant":{"epochSecond":1627987434,"nanoOfSecond":812000000},"thread":"pulsar-ordered-OrderedExecutor-0-0-EventThread","level":"INFO","loggerName":"stevlog.TOPICS.LOOKUP","message":"LOOKUP","endOfBatch":false,"loggerFqcn":"org.apache.logging.slf4j.Log4jLogger","contextMap":{"authoritative":"false","durationMs":"6","event":"LOOKUP","id":"57267001-e9f0-4c46-b7f0-db8048747a11","parentId":"e49858e2-fa24-4639-9a8d-1f01427697c1","traceId":"402ecc02-f186-4b0d-b741-462d2a6683c1","permitsAvailable":"50000","reqId":"1","topic":"persistent://public/default/foobar"},"threadId":217,"threadPriority":5}
```
### … prettified
```json
{
  "instant": {
    "epochSecond": 1627987434,
    "nanoOfSecond": 812000000
  },
  "thread": "pulsar-ordered-OrderedExecutor-0-0-EventThread",
  "level": "INFO",
  "loggerName": "org.apache.bookkeeper.slogger.slf4j.Slf4jSlogger",
  "message": "LOOKUP",
  "endOfBatch": false,
  "loggerFqcn": "org.apache.logging.slf4j.Log4jLogger",
  "contextMap": {
    "authoritative": "false",
    "durationMs": "6",
    "event": "LOOKUP",
    "id": "57267001-e9f0-4c46-b7f0-db8048747a11",
    "parentId": "e49858e2-fa24-4639-9a8d-1f01427697c1",
    "traceId": "402ecc02-f186-4b0d-b741-462d2a6683c1",
    "permitsAvailable": "50000",
    "reqId": "1",
    "topic": "persistent://public/default/foobar"
  },
  "threadId": 217,
  "threadPriority": 5
}
```
### With pattern appender
```
11:34:46.866 [pulsar-ordered-OrderedExecutor-7-0-EventThread] INFO stevlog.TOPICS.LOOKUP - LOOKUP {authoritative=false, durationMs=4, event=LOOKUP,id=9eff92a1-eafa-477c-a25f-83bc256213ee, parentId=e49858e2-fa24-4639-9a8d-1f01427697c1, traceId=402ecc02-f186-4b0d-b741-462d2a6683c1, permitsAvailable=50000, reqId=1, topic=persistent://public/default/foobar}
```


# Compatibility, Deprecation, and Migration Plan
Once the SLF4J implementation of the StEvLog wrapper is in, subsystems can be changed one by one. One potential strategy will be to look at what is being logged on a running cluster in the traditional logging format and replacing them. We will also focus on areas where lack of logging has been a pain-point in the past, such as load balancing and lookups.

The log output will change. People with text based logging configurations will need to ensure that MDC is in the logs. Users of log4j2 PatternLayout should ensure that %X exists in their pattern.

# Test Plan
The wrapper will be unit tested for correctness.

# Rejected Alternatives

Slf4j 2.0.0alpha does have structured logging. However it has the following shortcomings:
* It encourages string interpolation
* It doesn't allow context to be shared between log messages
* No support for sampling
* No support for event durations
* There is very little active development

For these reasons, we would have to wrap the API in any case, which is effectively what is proposed by this proposal.
