---
title: Deploying a Pulsar cluster on AWS using Ansible
tags: [admin, deployment, cluster, ansible]
---

{% include admonition.html type="info"
   content="For instructions on manually deploying a single Pulsar cluster, rather than using Ansible, see [Deploying a Pulsar cluster on bare metal](../cluster). For instructions on manually deploying a multi-cluster Pulsar instance, see [Deploying a Pulsar instance on bare metal](../instance)." %}

## Requirements

## What is installed

When you run the Ansible playbook, the following AWS resources will be used:

* 7 total [Elastic Compute Cloud](https://aws.amazon.com/ec2) (EC2) instances
  * 3 small VMs for ZooKeeper
  * 3 larger VMs for Pulsar {% popover brokers %} and BookKeeper {% popover bookies %}
* 