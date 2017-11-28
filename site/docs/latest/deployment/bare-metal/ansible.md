---
title: Deploying a Pulsar cluster on AWS using an Ansible playbook
tags: [admin, deployment, cluster, ansible]
---

{% include admonition.html type="info"
   content="For instructions on deploying a single Pulsar cluster manually rather than using Ansible, see [Deploying a Pulsar cluster on bare metal](../cluster). For instructions on manually deploying a multi-cluster Pulsar instance, see [Deploying a Pulsar instance on bare metal](../instance)." %}

One of the easiest ways to get a Pulsar {% popover cluster %} running on [Amazon Web Services](https://aws.amazon.com/) (AWS) is to use the the [Ansible](https://www.ansible.com) server automation tool. Pulsar's [GitHub]() repository.

## Requirements and setup

In order install a Pulsar cluster on AWS using Ansible, you'll need:

* An [AWS account](https://aws.amazon.com/account/)
* Python and [pip](https://pip.pypa.io/en/stable/)
* Ansible installed locally

You can install Ansible on Linux, macOS, or Windows using pip.

```bash
$ sudo pip install ansible
```

You'll also need to have the playbook locally. You can fetch it using Git:

```bash
$ git clone https://github.com/apache/incubator-pulsar
$ cd incubator-pulsar/ansible
```

## Running the Pulsar playbook

## What is installed

When you run the Ansible playbook, the following AWS resources will be used:

* 6 total [Elastic Compute Cloud](https://aws.amazon.com/ec2) (EC2) instances running the [ami-9fa343e7](https://access.redhat.com/articles/3135091) Amazon Machine Image (AMI), which runs [Red Hat Enterprise Linux (RHEL) 7.4](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html-single/7.4_release_notes/index). That includes:
  * 3 small VMs for ZooKeeper ([t2.small](https://www.ec2instances.info/?selected=t2.small) instances)
  * 3 larger VMs for Pulsar {% popover brokers %} and BookKeeper {% popover bookies %} ([i3.4xlarge](https://www.ec2instances.info/?selected=i3.4xlarge) instances)
* An EC2 [security group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
* A [virtual private cloud](https://aws.amazon.com/vpc/) (VPC) for security
* An [API Gateway](https://aws.amazon.com/api-gateway/) for connections from the outside world

These EC2 instances will be in the [us-west-2](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html) region