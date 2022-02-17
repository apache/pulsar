---
author: Enrico Olivelli
authorURL: https://twitter.com/eolivelli
title: Apache Pulsar 2.7.2
---
We are very glad to see the Apache Pulsar community has successfully released the 2.7.2 version.
This is a minor release that introduces stability fixes and a few new features without breaking changes.

<!--truncate-->

### News and noteworthy

Here is a selection of the most awesome and major enhancements added to Pulsar 2.7.2.

- Improvement in stability in the Kinesis connector [#10420](https://github.com/apache/pulsar/pull/10420).
- Improvement in passing ENV variables to the bookie (PULSAR_EXTRA_OPTS) [#10397](https://github.com/apache/pulsar/pull/10397).
- Allow the C++ client to be built in Windows and add CI for verification [#10387](https://github.com/apache/pulsar/pull/10387).
- Allow activating every BookKeeper client features in the broker [#9232](https://github.com/apache/pulsar/pull/9232).
- Improvement in Pulsar proxy.
- Upgrade core networking libraries: Jetty and Netty.

[Here](https://github.com/apache/pulsar/pulls?page=1&q=is%3Apr+label%3Arelease%2F2.7.2]) you can find the list of all the improvements and bug fixes.

### Contributors for 2.7.2 release

We would like to thank all the contributors for this release.
Same to other sustainable open source projects, Apache Pulsar is great because it is supported by a vibrant community.

Code contributors (names taken from GitHub API):
Ali Ahmed, Andrey Yegorov, Binbin Guo, David Kjerrumgaard, Deon van der Vyver, Devin Bost, Enrico Olivelli, Guangning E, Kevin Wilson,
Lari Hotari, Marvin Cai, Masahiro Sakamoto, Matteo Merli, Michael Marshall, Rajan Dhabalia, Shen Liu, Ting Yuan, Vincent Royer,
Yong Zhang, Yunze Xu, Zhanpeng Wu, Zike Yang, baomingyu, CongBo, dockerzhang, feynmanlin, hangc0276, li jinquan, limingnihao,
linlinnn, mlyahmed, PengHui Li, Ran.

Documentation contributors:
Anonymitaet (Yu Liu), Jennifer Huang

Also, we want to thank everyone who spent his time reporting issues and sharing the story about using Pulsar.

Looking forward to your contributions to [Apache Pulsar](https://github.com/apache/pulsar).
