---
id: functions-worker
title: Set up function workers
sidebar_label: "Set up function workers"
---

You have two ways to set up [function workers](function-concepts.md#function-worker). 
- [Run function workers with brokers](functions-worker-corun.md). Use it when:
    - resource isolation is not required when running functions in process or thread mode; 
    - you configure the function workers to run functions on Kubernetes (where the resource isolation problem is addressed by Kubernetes).
- [Run function workers separately](functions-worker-run-separately.md). Use it when you want to separate functions and brokers.

**Optional configurations**
* [Configure temporary file path](functions-worker-tmp-file-path.md)
* [Enable stateful functions](functions-worker-stateful.md)
* [Configure function workers for geo-replicated clusters](functions-worker-for-geo-replication.md)

**Reference**
* [Troubleshooting](functions-worker-troubleshooting.md)
