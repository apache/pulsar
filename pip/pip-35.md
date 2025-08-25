# PIP-35: Improve topic lookup for topics that have high number of partitions

* **Status**: Proposal
 * **Author**: Zhengxin Cai, Lin Lin
 * **Pull Request**: 
 * **Mailing List discussion**: http://mail-archives.apache.org/mod_mbox/pulsar-dev/201904.mbox/%3CCAHQi0xemFAGC6P9e240-8Ho0z4qtUv0uSBLtHfSs5ZeBDJXEug%40mail.gmail.com%3E

## Motivation

As this [github issue](https://github.com/apache/pulsar/issues/1088) brings up, for a partitioned topic, client has to send LookUp request for each partition. Here we propose a solution to make lookup easier for partitioned topic.

We could either make current LookUp(getBroker) api smarter and it will check if topic is a partitioned topic or not. If it’s a normal topic just return normal LookUp response which will be Pair<InetSocketAddress, InetSocketAddress>, if it’s a partitioned topic then it will return 
List<Pair<InetSocketAddress, InetSocketAddress>> contains all brokers that serving that partitioned topic.

Or we could create a new BatchLookUp api, like getBrokers. It can take a list of topic as input, where each topic has to be either a non partitioned topic or a partition of a partitioned topic and it’ll return Map<TopicName, Pair<InetSocketAddress, InetSocketAddress>> where it’s a map of input topic to corresponding address.

Prefer to go with second approach as it not only support lookup for all partitions of partitioned topic, it also support bulk lookup for many normal topics in one request.

## Proposal

In TopicLookupBase is where the core lookup logic will happens. 
Add new lookupTopicBatchAsync,  within the method, after validation, for each topic, we’ll fist find out the bundle it belong, then invoke NameSpaceService.findBrokerServiceUrl for each bundle and create a Map<Bundle , List<TopicName>>.
Then iterate through map and create response for each topicname.

In LookupService, when findBrokers invoke newBatchLookup,  it’ll get a response of a list<lookupresponse>, where some of response contains redirect, for these response will invoke findBrokers again to contact correct cluster. Combine the result of 2 invocation to get a final result.

Here’s proposed change to protocal buffer definition and apis:

*PulsarApi.proto*

```
	Add 
	Type enum:
		BATCH_LOOKUP           = 23;
		BATCH_LOOKUP_RESPONSE  = 24;

	BaseBommand:
		optional CommandBatchLookupTopic lookupTopic                    = 23;
		optional CommandBatchLookupTopicResponse lookupTopicResponse    = 24;

	CommandBatchLookupTopic:
	{
		repeated string topics           = 1;
		required uint64 request_id       = 2;
	}

	CommandTopicResponse:
	{	
		repeated string topicname               = 10;
	}
```



- Commands.java
 	- Add newBatchLookUp

- ClientCnx.java
	- Add newBatchLookup/handleBatchLookUpResponse

- ServiceCnx.java
	- Add handleBatchLookup

- LookupProxyHandler.java   
	- Add handleBatchLookup

- ProxyConnection.java
	- Add handleBatchLookup

- PulsarDecoder.java
	- Handle batchLookup

- Add BatchLookupResult.java

- TopicLookupBase.java
	- Add lookupTopicBatchAsync
```
List<CompletableFuture<ByteBuf>> lookupTopicBatchAsync(list<topics> topics) {
	List<CompletableFuture> resultFutures;
List<CompletableFuture> futures;
	Map<Bundle.toString, List<Topic> map;
            topics.foreach((topic) -> {
CompletableFuture validateFuture;
            CompletableFuture getNameSpaceBundleFuture;
CompletableFuture lookupFuture;

getClusterDataIfDifferentCluster.thenAccept(validateFuture.complete)
	validateFuture.thenAccept(validaitonFailureResponse -> {
		if(validaitonFailureResponse != null)
			lookupFuture.complete(validaitonFailureResponse)
pulsarService.getNamespaceService().getBundleAsync().
thenAccept(bundle -> map.get(bundle).add(requestId))

getNameSpaceBundleFuture.complete())

futures.add(getNameSpaceBundleFuture)
}}))
FutureUtil.waitForAll(futures).then(map.entry.foreach((entry) -> {
 	entry.key.findBrokerServiceUrl().thenAccept(lookupResult -> {
//Response will be address with success or redirect flag, and a list of topicnames in lookup request that belong to this bundle
		lookupFuture.complete(newLookupResponse()))
		resultFutures.add(lookupFuture)
})
}))

return resultFutures;
}
```

- TopicLookup.java(v2)
	- Add lookupTopicBatchAsync

- BinaryProto/HttpLookupService.java
	- Add getBrokers/findBrokers
