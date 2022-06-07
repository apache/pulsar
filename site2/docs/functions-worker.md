---
id: functions-worker
title: Set up function workers
sidebar_label: "Set up function workers"
---

You have two ways to set up [function workers](function-concepts.md#function-worker). 
- [run function workers with brokers](functions-worker-corun.md). Use it when:
    - resource isolation is not required when running functions in process or thread mode; 
    - you configure the function workers to run functions on Kubernetes (where the resource isolation problem is addressed by Kubernetes).
- [run function workers separately](functions-worker-run-separately.md). Use it when you want to separate functions and brokers.
