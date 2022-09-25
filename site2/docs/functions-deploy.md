---
id: functions-deploy
title: Deploy Pulsar Functions
sidebar_label: "How to deploy"
---

Pulsar provides two modes to deploy a function:
* [cluster mode (for production)](functions-deploy-cluster.md) - you can submit a function to a Pulsar cluster and the cluster will take charge of running the function. 
* [localrun mode](functions-deploy-localrun.md) - you can determine where a function runs, for example, on your local machine. 

## Prerequisites

Before deploying a function, you need to have a Pulsar cluster running first. You have the following options:
* Run a [standalone cluster](getting-started-standalone.md) locally on your own machine.
* Run a Pulsar cluster on [Kubernetes](deploy-kubernetes.md), [Amazon Web Services](deploy-aws.md), [bare metal](deploy-bare-metal.md), and so on.

:::note

If you want to deploy user-defined functions in Python, you need to install the [python client](client-libraries-python.md) on all the machines running [function workers](functions-concepts.md#function-worker).

:::
