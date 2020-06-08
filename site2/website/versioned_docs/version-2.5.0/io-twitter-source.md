---
id: version-2.5.0-io-twitter-source
title: Twitter Firehose source connector
sidebar_label: Twitter Firehose source connector
original_id: io-twitter-source
---

The Twitter Firehose source connector receives tweets from Twitter Firehose and 
writes the tweets to Pulsar topics.

## Configuration

The configuration of the Twitter Firehose source connector has the following properties.

### Property

| Name | Type|Required | Default | Description 
|------|----------|----------|---------|-------------|
| `consumerKey` | String|true | " " (empty string) | The twitter OAuth consumer key.<br><br>For more information, see [Access tokens](https://developer.twitter.com/en/docs/basics/authentication/guides/access-tokens). |
| `consumerSecret` | String |true | " " (empty string)  | The twitter OAuth consumer secret. |
| `token` | String|true | " " (empty string)  | The twitter OAuth token. |
| `tokenSecret` | String|true | " " (empty string) | The twitter OAuth secret. |
| `guestimateTweetTime`|Boolean|false|false|Most firehose events have null createdAt time.<br><br>If `guestimateTweetTime` set to true, the connector estimates the createdTime of each firehose event to be current time.
| `clientName` |  String |false | openconnector-twitter-source| The twitter firehose client name. |
| `clientHosts` |String| false | Constants.STREAM_HOST | The twitter firehose hosts to which client connects. |
| `clientBufferSize` | int|false | 50000 | The buffer size for buffering tweets fetched from twitter firehose. |

> For more information about OAuth credentials, see [Twitter developers portal](https://developer.twitter.com/en.html).
