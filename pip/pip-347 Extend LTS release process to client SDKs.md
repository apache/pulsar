# PIP-347: Extend LTS release process to client SDKs

## Motivation

PIP-175 [1] has introduced the concept of LTS releases in Pulsar, with a
defined cadence of feature and LTS release and setting a clear expectation
in terms of support window for each release.

The first Pulsar LTS release to follow this model was 3.0 in April 2023
and it will be followed by 4.0 release in October 2024.

Users have been very appreciative of these models and it has simplified the
work of maintainers, now that there are less active versions to support.

While for the Pulsar Java client SDK, we are implicitly adopting the LTS model, that
is not true for any other Pulsar client SDK, officially supported by the Pulsar
PMC.

This leaves all the non-Java client SDKs with different release cadences,
versioning and there is no support policy stated.


## Goal

The goal of this proposal is to align the support window for non-Java client
SDKs to the same LTS model introduced in PIP-175.

The interested clients are:
 * C++ https://github.com/apache/pulsar-client-cpp
 * Python https://github.com/apache/pulsar-client-python
 * Go https://github.com/apache/pulsar-client-go
 * NodeJS https://github.com/apache/pulsar-client-node
 * .NET https://github.com/apache/pulsar-dotpulsar
 * Reactive https://github.com/apache/pulsar-client-reactive

All the above client libraries should switch to the 4.0 version now, to mark the
first LTS release, then they will do 5.0 18 months after.

Users of these client SDK should be expecting bug fixes for 24 months and
security for 36 months on the LTS release branches.

This will set a clear expectation for users when deciding on a Pulsar SDK
version to use.


## Changes

All non-Java client SDKs managed by Pulsar PMC, will be starting to follow the
same LTS release cadence established by Pulsar server releases.

The alignment will be done only in terms of LTS releases. Feature releases will
be done independently, with no requirement for quarterly feature releases. Each
client SDK maintainer team (even though there is no formal team, there is
typically a smaller group of committers/PMCs closely following each language SDK)
will do feature and patch releases as needed.

The only new requirement will be to perform LTS releases every 18 months.

There will be no additional requirement or additional explicit support window
for client SDKs feature releases.


## Note

Similarly as for Pulsar server LTS releases, the major version digit bump does
not signify any breaking changes and does not necessarily imply any "big" new
features are introduced in a release.

The compatibility between client and brokers running in different major versions
will always be guaranteed, when using the lowest common denominator of
supported features.

