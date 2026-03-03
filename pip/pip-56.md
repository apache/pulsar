# PIP-56: Python3 Migration

* **Status**: Proposal
* **Author**: Ali Ahmed
* **Pull Request**:
* **Mailing List discussion**:
* **Release**:


## Context

Python 27 has reached end of life currently we currently distribute python 2.x artifacts in pypi
and python 27 is the default runtime in pulsar images.

As part of a migration effort I propose the following changes:
1) Move the default python version in the pulsar image from python27 to python37.
2) Move the integration test to use python37 runtime.
3) Advertise deprecation of python 37, provide support of python27 artifacts till end of 2020.

## Impact

Installations depending on python clients and users of python functions with the current image will be impacted there is no simple migration path if the default python installation changes. Test coverage of python27 will invariably be reduced as default integration test run against python 37 explicitly.

Users will have be notified with explicit release notes and be provided instructions to build custom images with python27 as the default. We should continue to publish python 27 artifacts till end of the year.

## Changes Needed

Changes are needed in few docker and pom files.

1) Removal of python 2 installation

https://github.com/apache/pulsar/blob/f0d339e67b551a63224c537cfe9518dc5b99574b/docker/pulsar/Dockerfile#L47

https://github.com/apache/pulsar/blob/f0d339e67b551a63224c537cfe9518dc5b99574b/docker/pulsar/Dockerfile#L55

https://github.com/apache/pulsar/blob/f0d339e67b551a63224c537cfe9518dc5b99574b/docker/pulsar/Dockerfile#L60

https://github.com/apache/pulsar/blob/f0d339e67b551a63224c537cfe9518dc5b99574b/dashboard/Dockerfile#L25

2) Add symlinks to make python3 -> python and pip3 -> pip

3) Remove python 27 wheel installation
