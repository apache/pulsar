# PIP-324: Switch to Alpine Linux base Docker images


# Motivation

Pulsar Docker images are currently based on Ubuntu base images. While these images has served us well in the past years,
there are few shortcomings.

Alpine Linux is a Linux distribution designed explicitly to work well in container environments and has strong
focus on security and a minimalistic set of included dependencies.

### Size of the image

Ubuntu images come with a much larger set of pre-installed tools. In many cases these are not actually needed by Pulsar,
and it's better not include anything in the container images unless it's strictly required.

Example of minimal image size:
``` 
$ docker images | egrep 'ubuntu|alpine'
alpine                                                       3.19                     1dc785547989   4 days ago     7.73MB
ubuntu                                                       22.04                    031631b93326   11 days ago    69.3MB
```


Similarly, also the packaged that can be installed in Alpine are generally much smaller than the corresponding Ubuntu 
packages. In a complex image like the Pulsar one, this quickly adds up to hundreds of MBs.

Comparison between the 2 base images with only the Java runtime added (JRE):

```
alpine-jre                                                   latest                   eb0e093ee71c   29 seconds ago   211MB
ubuntu-jre                                                   latest                   4147e1b2c6d1   7 seconds ago    377MB
```

Size of Docker images is very important, because these images end up being stored in many registries and downloaded 
a million of times, bringing a concern in costs for network transfer as well as for storage. Additionally, in many cases
how fast is an image to download will determine the time it takes to spin up a new container in a new virtual machines 
(eg: when scaling a cluster up in response to a traffic increase). 

### Security posture

By starting with a minimal set of pre-installed tools, Alpine reduces the surface for security issues in the base image.

At this moment there are 12 Medium/Low CVEs opened in Ubuntu for which there is no resolution available. Some of these
CVEs have been opened for many months.
Even though these CVEs don't look particularly dangerous and might not apply in 100% of cases to the Pulsar deployment, 
they will still be flagged in every security review, and they will trigger an in-depth investigation and require ad-hoc 
approvals.

At the same time, there are 0 CVEs in the Alpine image.

```
~ docker scout quickview ubuntu:22.04
    ! New version 1.2.2 available (installed version is 1.0.9) at https://github.com/docker/scout-cli
    ✓ SBOM of image already cached, 143 packages indexed

  Target   │  ubuntu:22.04  │    0C     0H     2M    10L
    digest │  031631b93326  │
```

```
~ docker scout quickview alpine:3.19.0
    ! New version 1.2.2 available (installed version is 1.0.9) at https://github.com/docker/scout-cli
    ✓ SBOM of image already cached, 19 packages indexed

  Target   │  alpine:3.19.0  │    0C     0H     0M     0L
    digest │  1dc785547989   │
```

# Goals

## In Scope

Convert the tooling that produces the Pulsar Docker image to use Alpine as the 

## Out of Scope

As part of this PIP there will be no explicit work to reduce the size of the Docker image, other than the conversion
of the base image. This could be done as part of further initiatives.

# High Level Design

The base of `apachepulsar/pulsar` will be converted to use Alpine Linux base image. All the other images that are part
of the Pulsar projects will be updated to make sure they can work correctly (eg: use `apk add` instead of `apt install`).

Release notes for Pulsar 3.X.0 release will include note to notify downstream users, who might be doing some advanced 
customizations to the official Apache Pulsar images. This should be a tiny minority of users though. In most cases, 
users will see no visible change, and will not have to perform any extra step of configuration change during the upgrade
from an Ubuntu based image to an Alpine based image.

# Detailed Design

## Public-facing Changes

### Public API

No changes

### Binary protocol

No changes

### Configuration

No changes

### CLI

No changes

### Metrics

No changes

# Monitoring

No changes

# Security Considerations
<!--
A detailed description of the security details that ought to be considered for the PIP. This is most relevant for any new HTTP endpoints, new Pulsar Protocol Commands, and new security features. The goal is to describe details like which role will have permission to perform an action.

An important aspect to consider is also multi-tenancy: Does the feature I'm adding have the permissions / roles set in such a way that prevent one tenant accessing another tenant's data/configuration? For example, the Admin API to read a specific message for a topic only allows a client to read messages for the target topic. However, that was not always the case. CVE-2021-41571 (https://github.com/apache/pulsar/wiki/CVE-2021-41571) resulted because the API was incorrectly written and did not properly prevent a client from reading another topic's messages even though authorization was in place. The problem was missing input validation that verified the requested message was actually a message for that topic. The fix to CVE-2021-41571 was input validation. 

If there is uncertainty for this section, please submit the PIP and request for feedback on the mailing list.
-->

# Backward & Forward Compatibility

## Revert

No compatibility problems.

## Upgrade

No difference from a regular upgrade.


# Links

<!--
Updated afterwards
-->
* Mailing List discussion thread:
* Mailing List voting thread:
