---
id: version-2.7.2-deploy-aws
title: Deploying a Pulsar cluster on AWS using Terraform and Ansible
sidebar_label: Amazon Web Services
original_id: deploy-aws
---

> For instructions on deploying a single Pulsar cluster manually rather than using Terraform and Ansible, see [Deploying a Pulsar cluster on bare metal](deploy-bare-metal.md). For instructions on manually deploying a multi-cluster Pulsar instance, see [Deploying a Pulsar instance on bare metal](deploy-bare-metal-multi-cluster.md).

One of the easiest ways to get a Pulsar [cluster](reference-terminology.md#cluster) running on [Amazon Web Services](https://aws.amazon.com/) (AWS) is to use the [Terraform](https://terraform.io) infrastructure provisioning tool and the [Ansible](https://www.ansible.com) server automation tool. Terraform can create the resources necessary for running the Pulsar cluster---[EC2](https://aws.amazon.com/ec2/) instances, networking and security infrastructure, etc.---While Ansible can install and run Pulsar on the provisioned resources.

## Requirements and setup

In order to install a Pulsar cluster on AWS using Terraform and Ansible, you need to prepare the following things:

* An [AWS account](https://aws.amazon.com/account/) and the [`aws`](https://aws.amazon.com/cli/) command-line tool
* Python and [pip](https://pip.pypa.io/en/stable/)
* The [`terraform-inventory`](https://github.com/adammck/terraform-inventory) tool, which enables Ansible to use Terraform artifacts

You also need to make sure that you are currently logged into your AWS account via the `aws` tool:

```bash
$ aws configure
```

## Installation

You can install Ansible on Linux or macOS using pip.

```bash
$ pip install ansible
```

You can install Terraform using the instructions [here](https://www.terraform.io/intro/getting-started/install.html).

You also need to have the Terraform and Ansible configuration for Pulsar locally on your machine. You can find them in the [GitHub repository](https://github.com/apache/pulsar) of Pulsar, which you can fetch using Git commands:

```bash
$ git clone https://github.com/apache/pulsar
$ cd pulsar/deployment/terraform-ansible/aws
```

## SSH setup

> If you already have an SSH key and want to use it, you can skip the step of generating an SSH key and update `private_key_file` setting
> in `ansible.cfg` file and `public_key_path` setting in `terraform.tfvars` file.
>
> For example, if you already have a private SSH key in `~/.ssh/pulsar_aws` and a public key in `~/.ssh/pulsar_aws.pub`,
> follow the steps below:
>
> 1. update `ansible.cfg` with following values:
>
> ```shell
> private_key_file=~/.ssh/pulsar_aws
> ```
>
> 2. update `terraform.tfvars` with following values:
>
> ```shell
> public_key_path=~/.ssh/pulsar_aws.pub
> ```

In order to create the necessary AWS resources using Terraform, you need to create an SSH key. Enter the following commands to create a private SSH key in `~/.ssh/id_rsa` and a public key in `~/.ssh/id_rsa.pub`:

```bash
$ ssh-keygen -t rsa
```

Do *not* enter a passphrase (hit **Enter** instead when the prompt comes out). Enter the following command to verify that a key has been created:

```bash
$ ls ~/.ssh
id_rsa               id_rsa.pub
```

## Create AWS resources using Terraform

To start building AWS resources with Terraform, you need to install all Terraform dependencies. Enter the following command:

```bash
$ terraform init
# This will create a .terraform folder
```

After that, you can apply the default Terraform configuration by entering this command:

```bash
$ terraform apply
```

Then you see this prompt below:

```bash
Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value:
```

Type `yes` and hit **Enter**. Applying the configuration could take several minutes. When the configuration applying finishes, you can see `Apply complete!` along with some other information, including the number of resources created.

### Apply a non-default configuration

You can apply a non-default Terraform configuration by changing the values in the `terraform.tfvars` file. The following variables are available:

Variable name | Description | Default
:-------------|:------------|:-------
`public_key_path` | The path of the public key that you have generated. | `~/.ssh/id_rsa.pub`
`region` | The AWS region in which the Pulsar cluster runs | `us-west-2`
`availability_zone` | The AWS availability zone in which the Pulsar cluster runs | `us-west-2a`
`aws_ami` | The [Amazon Machine Image](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html) (AMI) that the cluster uses  | `ami-9fa343e7`
`num_zookeeper_nodes` | The number of [ZooKeeper](https://zookeeper.apache.org) nodes in the ZooKeeper cluster | 3
`num_bookie_nodes` | The number of bookies that runs in the cluster | 3
`num_broker_nodes` | The number of Pulsar brokers that runs in the cluster | 2
`num_proxy_nodes` | The number of Pulsar proxies that runs in the cluster | 1
`base_cidr_block` | The root [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) that network assets uses for the cluster | `10.0.0.0/16`
`instance_types` | The EC2 instance types to be used. This variable is a map with two keys: `zookeeper` for the ZooKeeper instances, `bookie` for the BookKeeper bookies and `broker` and `proxy` for Pulsar brokers and bookies | `t2.small` (ZooKeeper), `i3.xlarge` (BookKeeper) and `c5.2xlarge` (Brokers/Proxies)

### What is installed

When you run the Ansible playbook, the following AWS resources are used:

* 9 total [Elastic Compute Cloud](https://aws.amazon.com/ec2) (EC2) instances running the [ami-9fa343e7](https://access.redhat.com/articles/3135091) Amazon Machine Image (AMI), which runs [Red Hat Enterprise Linux (RHEL) 7.4](https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html-single/7.4_release_notes/index). By default, that includes:
  * 3 small VMs for ZooKeeper ([t2.small](https://www.ec2instances.info/?selected=t2.small) instances)
  * 3 larger VMs for BookKeeper [bookies](reference-terminology.md#bookie) ([i3.xlarge](https://www.ec2instances.info/?selected=i3.xlarge) instances)
  * 2 larger VMs for Pulsar [brokers](reference-terminology.md#broker) ([c5.2xlarge](https://www.ec2instances.info/?selected=c5.2xlarge) instances)
  * 1 larger VMs for Pulsar [proxy](reference-terminology.md#proxy) ([c5.2xlarge](https://www.ec2instances.info/?selected=c5.2xlarge) instances)
* An EC2 [security group](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-network-security.html)
* A [virtual private cloud](https://aws.amazon.com/vpc/) (VPC) for security
* An [API Gateway](https://aws.amazon.com/api-gateway/) for connections from the outside world
* A [route table](http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_Route_Tables.html) for the Pulsar cluster's VPC
* A [subnet](http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/VPC_Subnets.html) for the VPC

All EC2 instances for the cluster run in the [us-west-2](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html) region.

### Fetch your Pulsar connection URL

When you apply the Terraform configuration by entering the command `terraform apply`, Terraform outputs a value for the `pulsar_service_url`. The value should look something like this:

```
pulsar://pulsar-elb-1800761694.us-west-2.elb.amazonaws.com:6650
```

You can fetch that value at any time by entering the command `terraform output pulsar_service_url` or parsing the `terraform.tstate` file (which is JSON, even though the filename does not reflect that):

```bash
$ cat terraform.tfstate | jq .modules[0].outputs.pulsar_service_url.value
```

### Destroy your cluster

At any point, you can destroy all AWS resources associated with your cluster using Terraform's `destroy` command:

```bash
$ terraform destroy
```

## Setup Disks

Before you run the Pulsar playbook, you need to mount the disks to the correct directories on those bookie nodes. Since different type of machines have different disk layout, you need to update the task defined in `setup-disk.yaml` file after changing the `instance_types` in your terraform config,

To setup disks on bookie nodes, enter this command:

```bash
$ ansible-playbook \
  --user='ec2-user' \
  --inventory=`which terraform-inventory` \
  setup-disk.yaml
```

After that, the disks is mounted under `/mnt/journal` as journal disk, and `/mnt/storage` as ledger disk.
Remember to enter this command just only once. If you attempt to enter this command again after you have run Pulsar playbook, your disks might potentially be erased again, causing the bookies to fail to start up.

## Run the Pulsar playbook

Once you have created the necessary AWS resources using Terraform, you can install and run Pulsar on the Terraform-created EC2 instances using Ansible. 

(Optional) If you want to use any [built-in IO connectors](io-connectors.md) , edit the `Download Pulsar IO packages` task in the `deploy-pulsar.yaml` file and uncomment the connectors you want to use. 

To run the playbook, enter this command:

```bash
$ ansible-playbook \
  --user='ec2-user' \
  --inventory=`which terraform-inventory` \
  ../deploy-pulsar.yaml
```

If you have created a private SSH key at a location different from `~/.ssh/id_rsa`, you can specify the different location using the `--private-key` flag in the following command:

```bash
$ ansible-playbook \
  --user='ec2-user' \
  --inventory=`which terraform-inventory` \
  --private-key="~/.ssh/some-non-default-key" \
  ../deploy-pulsar.yaml
```

## Access the cluster

You can now access your running Pulsar using the unique Pulsar connection URL for your cluster, which you can obtain following the instructions [above](#fetching-your-pulsar-connection-url).

For a quick demonstration of accessing the cluster, we can use the Python client for Pulsar and the Python shell. First, install the Pulsar Python module using pip:

```bash
$ pip install pulsar-client
```

Now, open up the Python shell using the `python` command:

```bash
$ python
```

Once you are in the shell, enter the following command:

```python
>>> import pulsar
>>> client = pulsar.Client('pulsar://pulsar-elb-1800761694.us-west-2.elb.amazonaws.com:6650')
# Make sure to use your connection URL
>>> producer = client.create_producer('persistent://public/default/test-topic')
>>> producer.send('Hello world')
>>> client.close()
```

If all of these commands are successful, Pulsar clients can now use your cluster!
