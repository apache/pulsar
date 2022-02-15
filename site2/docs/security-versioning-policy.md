---
id: security-policy-and-supported-versions
title: Security Policy and Supported Versions
sidebar_label: Security Policy and Supported Versions
---

## Reporting a Vulnerability

The current process for reporting vulnerabilities is outlined here: https://www.apache.org/security/. When reporting a
vulnerability to security@apache.org, you can copy your email to [private@pulsar.apache.org](mailto:private@pulsar.apache.org)
to send your report to the Apache Pulsar Project Management Committee. This is a private mailing list.

## Using Pulsar's Security Features

You can find documentation on Pulsar's available security features and how to use them here:
https://pulsar.apache.org/docs/en/security-overview/.

## Security Vulnerability Announcements

The Pulsar community will announce security vulnerabilities and how to mitigate them on the [users@pulsar.apache.org](mailto:users@pulsar.apache.org).
For instructions on how to subscribe, please see https://pulsar.apache.org/contact/.

## Versioning Policy

The Pulsar project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html). Existing releases can expect
patches for bugs and security vulnerabilities. New features will target minor releases.

When upgrading an existing cluster, it is important to upgrade components linearly through each minor version. For
example, when upgrading from 2.8.x to 2.10.x, it is important to upgrade to 2.9.x before going to 2.10.x.

## Supported Versions

Feature release branches will be maintained with security fix and bug fix releases for a period of at least 12 months
after initial release. For example, branch 2.5.x is no longer considered maintained as of January 2021, 12 months after
the release of 2.5.0 in January 2020. No more 2.5.x releases should be expected at this point, even to fix security
vulnerabilities.

Note that a minor version can be maintained past it's 12 month initial support period. For example, version 2.7 is still
actively maintained.

Security fixes will be given priority when it comes to back porting fixes to older versions that are within the
supported time window. It is challenging to decide which bug fixes to back port to old versions. As such, the latest
versions will have the most bug fixes.

When 3.0.0 is released, the community will decide how to continue supporting 2.x. It is possible that the last minor
release within 2.x will be maintained for longer as an “LTS” release, but it has not been officially decided.

The following table shows version support timelines and will be updated with each release.

| Version | Supported          | Initial Release | At Least Until |
|:-------:|:------------------:|:---------------:|:--------------:|
| 2.9.x   | :white_check_mark: | November 2021   | November 2022  |
| 2.8.x   | :white_check_mark: | June 2021       | June 2022      |
| 2.7.x   | :white_check_mark: | November 2020   | November 2021  |
| 2.6.x   | :x:                | June 2020       | June 2021      |
| 2.5.x   | :x:                | January 2020    | January 2021   |
| 2.4.x   | :x:                | July 2019       | July 2020      |
| < 2.3.x | :x:                | -               | -              |

If there is ambiguity about which versions of Pulsar are actively supported, please ask on the [users@pulsar.apache.org](mailto:users@pulsar.apache.org)
mailing list.

## Release Frequency

With the acceptance of [PIP-47 - A Time Based Release Plan](https://github.com/apache/pulsar/wiki/PIP-47%3A-Time-Based-Release-Plan),
the Pulsar community aims to complete 4 minor releases each year. Patch releases are completed based on demand as well
as need, in the event of security fixes.
