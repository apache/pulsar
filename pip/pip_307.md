# Background knowledge

WebSocket currently only supports the consumption of a single topic, which cannot satisfy users' consumption scenarios of multiple topics.

# Motivation

Supports consumption of multiple topics or pattern topics.


# Detailed Design

Currently, the topic name is specified through path for consumption, like:
```
/ws/v2/consumer/persistent/my-property/my-ns/my-topic/my-subscription
```
If we want to support subscribing multi-topics, adding parameters will be confusing. Therefore, add a new v3 request path as follows:

For consumption of pattern-topics:
```
/ws/v3/consumer/subscription?topicsPattern="a.*"
```
For consumption of multi-topics:
```
/ws/v3/consumer/subscription?topics="a,b,c"
```

# Links

* Mailing List discussion thread: https://lists.apache.org/thread/co8396ywny161x91dffzvxlt993mo1ht
* Mailing List voting thread: https://lists.apache.org/thread/lk28o483y351s7m44p018320gq3g4507
