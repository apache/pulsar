# PIP-94: Message converter at broker level

- Status: Draft
- Author: Xiaolong Ran
- Pull request:
- Mailing list discussion:
- Release:

The purpose of this proposal is mainly to add ReconsumerLater on the consumer side to retry in an incremental level

## Motivation

At present, ReconsumrLater only supports specifying a specific delay time for distribution processing. The usage is as follows:

```
while (true) {
     Message<String> msg = consumer.receive();

     try {
          // Process message...

          consumer.acknowledge(msg);
     } catch (Throwable t) {
          log.warn("Failed to process message");
          consumer.reconsumeLater(msg, 1000 , TimeUnit.MILLISECONDS);
     }
 }
```

Its implementation principle is to use Pulsar's built-in delay message to pass in the specified time as the parameter 
of deliverAfter(), and then push the message to the consumer side again after the time arrives. 

This is a good idea, which allows users to flexibly define their own delay time in a specific scenario. But assuming 
that the message is not processed correctly within the time specified by the user, the behavior of ReconsumerLater has 
ended at this time. Whether we can consider adding a retry scheme according to the time level. Then when the first 
specified time range is not processed correctly, ReconsumerLater() can automatically retry according to the time level 
until the user correctly processes the specific message.

## Implementation

As mentioned above, if we can here follow a certain delay level from low to high and allow it to automatically retry, 
it is a more user-friendly way. For example, we can define the following delay level:

```
MESSAGE_DELAYLEVEL = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
```


In this PIP, we mainly introduce two new API interfaces to users:

1. Specify the delay level

```
reconsumeLater(Message<?> message, int delayLevel)
```

This implementation method is consistent with the current reconsumeLater interface, but instead of specifying the 
delay level, specify the specific delay time. For example, level `1` corresponds to `1s`, and level `3` corresponds to `10s`.


2. Retry with increasing level

```
reconsumeLater(Message<?> message)
```

Different from the above two, it is a back-off retry, that is, the retry interval after the first failure is 1 second, 
and the retry interval after the second failure is 5 seconds, and so on, the more the number of times, the longer the 
interval.

This kind of retry mechanism often has more practical applications in business scenarios. If the consumption fails, 
the general service will not be restored immediately. It is more reasonable to use this gradual retry method.


## Compatibility

The current proposal will not cause damage to compatibility. It exposes two new API interfaces based on the 
original API for users to use, so there will be no compatibility-related issues here.
