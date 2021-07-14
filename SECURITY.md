# Security Policy

## Supported Versions

Feature release branches will, generally, be maintained with security fix and bug fix releases for a period of 12 months
after initial release. For example, branch 2.5.x is no longer considered maintained as of January 2021, 12 months after
the release of 2.5.0 in January 2020. No more 2.5.x releases should be expected at this point, even for security fixes.

Security fixes will be given priority when it comes to back porting fixes to older versions that are within the
supported time window. It is challenging to decide which bug fixes to back port to old versions. As such, the latest
versions will have the most bug fixes.

When 3.0.0 is released, the community will decide how to continue supporting 2.x. It is possible that the last minor
release within 2.x will be maintained for longer as an “LTS” release, but it has not been officially decided.

The following table shows version support timelines and will be updated with releases.

| Version | Supported          | Initial Release | Until         |
|:-------:|:------------------:|:---------------:|:-------------:|
| 2.7.x   | :white_check_mark: | November 2020   | November 2021 |
| 2.6.x   | :white_check_mark: | June 2020       | June 2021     |
| 2.5.x   | :x:                | January 2020    | January 2021  |
| 2.4.x   | :x:                | July 2019       | July 2020     |
| < 2.3.x | :x:                | -               | -             |

## Release Frequency

With the acceptance of [PIP-47 - A Time Based Release Plan](https://github.com/apache/pulsar/wiki/PIP-47%3A-Time-Based-Release-Plan),
the Pulsar community aims to complete 4 minor releases each year.

## Reporting a Vulnerability

The current process for reporting vulnerabilities is outlined here: https://www.apache.org/security/.

## Using Pulsar's Security Features

You can find documentation on Pulsar's available security features and how to use them here: 
https://pulsar.apache.org/docs/en/security-overview/.