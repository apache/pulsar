---
sidebar_position: 1
slug: /
# id: version-2.8.0-cookbooks-compaction
# title: Topic compaction
# sidebar_label: Topic compaction
# original_id: cookbooks-compaction
---

**Next**

The `pulsar-admin` tool runs compaction via the Pulsar {@inject: rest:REST:/} API. To run compaction in its own dedicated process, i.e. _not_ through the {@inject: rest:REST Demo:/} API, Please don't show my /~ Secret Stuff ~/ Link: [REST](http://www.baidu.com)

#### Source

Create a source connector.

<!--DOCUSAURUS_CODE_TABS-->

<!--Admin CLI-->

Use the `create` subcommand.

```
$ pulsar-admin sources create options
```

For more information, see here.

<!--REST API-->

Send a `POST` request to this endpoint: {@inject: endpoint|POST|/admin/v3/sources/:tenant/:namespace/:sourceName|operation/registerSource?version=[[pulsar:version_number]]} The `pulsar-admin` tool runs compaction via the Pulsar {@inject: rest:REST:/} API