---
title: Deploying a Pulsar cluster on AWS using Terraform and Ansible
tags: [admin, deployment, cluster, ansible, terraform]
---

{% include admonition.html type="info"
   content="For instructions on deploying a single Pulsar cluster manually rather than using Terraform and Ansible, see [Deploying a Pulsar cluster on bare metal](../cluster). For instructions on manually deploying a multi-cluster Pulsar instance, see [Deploying a Pulsar instance on bare metal](../instance)." %}

One of the easiest ways to get a Pulsar {% popover cluster %} running on [Amazon Web Services](https://aws.amazon.com/) (AWS) is to use the the [Terraform](https://terraform.io) infrastructure provisioning tool and the [Ansible](https://www.ansible.com) server automation tool. Terraform can create the resources necessary to run the Pulsar cluster---[EC2](https://aws.amazon.com/ec2/) instances, networking and security infrastructure, etc.---while Ansible can install and run Pulsar on the provisioned resources.

## Requirements and setup

In order install a Pulsar cluster on AWS using Terraform and Ansible, you'll need:

* An [AWS account](https://aws.amazon.com/account/) and the [`aws`](https://aws.amazon.com/cli/) command-line tool
* Python and [pip](https://pip.pypa.io/en/stable/)
* The [`terraform-inventory`](https://github.com/adammck/terraform-inventory) tool, which enables Ansible to use Terraform artifacts

You'll also need to make sure that you're currently logged into your AWS account via the `aws` tool:

```bash
$ aws configure
```

## Installation

You can install Ansible on Linux or macOS using pip.

```bash
$ pip install ansible
```

You can install Terraform using the instructions [here](https://www.terraform.io/intro/getting-started/install.html).

You'll also need to have the Terraform and Ansible configurations for Pulsar locally on your machine. They're contained in Pulsar's [GitHub repository](https://github.com/apache/incubator-pulsar), which you can fetch using Git:

```bash
$ git clone https://github.com/apache/incubator-pulsar
$ cd incubator-pulsar/deployment/terraform-ansible/aws
```

## SSH setup

In order to create the necessary AWS resources using Terraform, you'll need to create an SSH key. To create a private SSH key in `~/.ssh/id_rsa` and a public key in `~/.ssh/id_rsa.pub`:

```bash
$ ssh-keygen -t rsa
```

Do *not* enter a passphrase (hit **Enter** when prompted instead). To verify that a key has been created:

```bash
$ ls ~/.ssh
id_rsa               id_rsa.pub
```

## Creating AWS resources using Terraform

To get started building AWS resources with Terraform, you'll need to install all Terraform dependencies:

```bash
$ terraform init
# This will create a .terraform folder
```

Once you've done that, you can apply the default Terraform configuration:

```bash
$ terraform apply
```

You should then see this prompt:

```bash
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```

Type `yes` and hit **Enter**. Applying the configuration could take several minutes. When it's finished, you should see `Apply complete!` along with some other information, including the number of resources created.

### Applying a non-default configuration

You can apply a non-default Terraform configuration by changing the values in the `terraform.tfvars` file. The following variables are available:

Variable name | Description | Default
:-------------|:------------|:-------
`public_key_path` | The path of the public key that you've generated. | `~/.ssh/id_rsa.pub`
`region` | The AWS region in which the Pulsar cluster will run | `us-west-2`
`availability_zone` | The AWS availability zone in which the Pulsar cluster will run | `us-west-2a`
`aws_ami` | The [Amazon Machine Image](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html) (AMI) that will be used by the cluster | `ami-9fa343e7`
`num_zookeeper_nodes` | The number of [ZooKeeper](https://zookeeper.apache.org) nodes in the ZooKeeper cluster | 3
`num_pulsar_brokers` | The number of Pulsar brokers and BookKeeper bookies that will run in the cluster | 3
`base_cidr_block` | The root [CIDR](http://searchnetworking.techtarget.com/definition/CIDR) that will be used by network assets for the cluster | `10.0.0.0/16`
`instance_types` | The EC2 instance types to be used. This variable is a map with two keys: `zookeeper` for the ZooKeeper instances and `pulsar` for the Pulsar brokers and BookKeeper bookies | `t2.small` (ZooKeeper) and `i3.xlarge` (Pulsar/BookKeeper)

### What is installed

When you run the Ansible playbook, the following AWS resources will be used:

* 6 total [Elastic Compute Cloud](https://aws.amazon.com/ec2) (EC2) instances running the [ami-9fa343e7](https://access.redhat.com/articles/3135091) Amazon Machine Image (AMI), which runs [Red Hat Enterprise Linux (RHEL) 7.4](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html-single/7.4_release_notes/index). By default, that includes:
  * 3 small VMs for ZooKeeper ([t2.small](https://www.ec2instances.info/?selected=t2.small) instances)
  * 3 larger VMs for Pulsar {% popover brokers %} and BookKeeper {% popover bookies %} ([i3.4xlarge](https://www.ec2instances.info/?selected=i3.4xlarge) instances)
* An EC2 [security group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
* A [virtual private cloud](https://aws.amazon.com/vpc/) (VPC) for security
* An [API Gateway](https://aws.amazon.com/api-gateway/) for connections from the outside world
* A [route table](http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_Route_Tables.html) for the Pulsar cluster's VPC
* A [subnet](http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_Subnets.html) for the VPC

All EC2 instances for the cluster will run in the [us-west-2](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html) region.

### Fetching your Pulsar connection URL

When you apply the Terraform configuration by running `terraform apply`, Terraform will output a value for the `pulsar_service_url`. It should look something like this:

```
pulsar://pulsar-elb-1800761694.us-west-2.elb.amazonaws.com:6650
```

You can fetch that value at any time by running `terraform output pulsar_service_url` or parsing the `terraform.tstate` file (which is JSON, even though the filename doesn't reflect that):

```bash
$ cat terraform.tfstate | jq .modules[0].outputs.pulsar_service_url.value
```

### Destroying your cluster

At any point, you can destroy all AWS resources associated with your cluster using Terraform's `destroy` command:

```bash
$ terraform destroy
```

## Running the Pulsar playbook

Once you've created the necessary AWS resources using Terraform, you can install and run Pulsar on the Terraform-created EC2 instances using Ansible. To do so, use this command:

```bash
$ ansible-playbook \
  --user='ec2-user' \
  --inventory=`which terraform-inventory` \
  ../deploy-pulsar.yaml
```

If you've created a private SSH key at a location different from `~/.ssh/id_rsa`, you can specify the different location using the `--private-key` flag:

```bash
$ ansible-playbook \
  --user='ec2-user' \
  --inventory=`which terraform-inventory` \
  --private-key="~/.ssh/some-non-default-key" \
  ../deploy-pulsar.yaml
```

## Accessing the cluster

You can now access your running Pulsar using the unique Pulsar connection URL for your cluster, which you can obtain using the instructions [above](#fetching-your-pulsar-connection-url).

For a quick demonstration of accessing the cluster, we can use the Python client for Pulsar and the Python shell. First, install the Pulsar Python module using pip:

```bash
$ pip install pulsar-client
```

Now, open up the Python shell using the `python` command:

```bash
$ python
```

Once in the shell, run the following:

```python
>>> import pulsar
>>> client = pulsar.Client('pulsar://pulsar-elb-1800761694.us-west-2.elb.amazonaws.com:6650')
# Make sure to use your connection URL
>>> producer = client.create_producer('persistent://public/default/test-topic')
>>> producer.send('Hello world')
>>> client.close()
```

If all of these commands are successful, your cluster can now be used by Pulsar clients!
