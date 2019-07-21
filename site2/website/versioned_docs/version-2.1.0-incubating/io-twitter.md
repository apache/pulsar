---
id: version-2.1.0-incubating-io-twitter
title: Twitter Firehose Connector
sidebar_label: Twitter Firehose Connector
original_id: io-twitter
---

The Twitter Firehose connector is used for receiving tweets from Twitter Firehose and writing
the tweets to Pulsar topics.

## Source Configuration Options

You can get the OAuth credentials from [Twitter Developers Portal](https://developer.twitter.com/en.html).

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| consumerKey | true | null | Twitter OAuth Consumer Key |
| consumerSecret | true | null | Twitter OAuth Consumer Secret |
| token | true | null | Twitter OAuth Token |
| tokenSecret | true | null | Twitter OAuth Secret |
| clientName | false | `openconnector-twitter-source"`| Client name |
| clientHosts | false | `https://stream.twitter.com` | Twitter Firehose hosts that client connects to |
| clientBufferSize | false | `50000` | The buffer size for buffering tweets fetched from Twitter Firehose |


