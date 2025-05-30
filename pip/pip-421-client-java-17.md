# PIP-421: Require Java 17 as the minimum for Pulsar Java client SDK

# Context

Currently, Pulsar requires Java 17 for the server side components and Java 8 for the client SDK and the 
client admin SDK.

For the server side components the change was done in PIP-156 [1] in April 2022. At the time it was
deemed too early and not necessary to require Java 17 for client SDK as well. 

There has been a discussion in February 2023 as well [2] where the consensus still was to keep supporting Java 8.

# Motivation

Since the previous discussions, there have been several changes in the Java & Pulsar world:

 1. Java 8 has been out of premier support for 3 years already [3] and its usage has been drastically decreasing
    over the years, from 85% in 2020, 40% in 2023 and 23% in 2024 [4]. All indicate that by 2028, usage of Java 8
    will be negligible.
 2. Java 17 LTS was released ~4 years ago, and it's quite widely adopted in Java production environments, 
    along with Java 21 LTS. 
 3. Pulsar introduced the concept of LTS release which does get support for 2-3 years. This means that a change 
    we make now will not really affect users sooner than the current LTS goes out of the support window.


## Issues with dependencies

Many popular Java libraries have started switching to requiring Java >= 11 or >= 17. This is posing 
a real problem because we are stuck into old and unsupported versions. When there is a CVE flagged
in these dependencies, we don't have any way to upgrade to a patched version.

Non-exhaustive set of libraries requiring Java >= 11:

 * Jetty 12 - We are currently using Jetty 9.x, which is completely unsupported at this point and 
   there are active CVEs in the version we use.
 * Jersey 3.1 - In order to upgrade to Jetty 12, we'd need to upgrade Jersey as well.
 * Jakarta APIs - All new APIs for WS and Rest require Java 11.
 * AthenZ - This is an optional dependency for authentication, though all new versions require Java 17.

There are certainly more dependencies we are using today that have already switched new versions 
to Java 17. This will pose a growing risk for the near future.

### Why Java 17 instead of jumping to 11

The assumption is that the vast majority of Java users have made migrations directly from 8 to 17. Java 11 
has already stopped the premier support, so there would be no strong reason to settle on 11. 

# Changes

 1. From Pulsar 4.1, require Java >= 17 for all client modules
 2. Pulsar 4.0 will continue with the current status of requiring Java 8 for clients. This will give an 
    additional 3 years for users that are stuck on Java 8, up to 2028. 
 3. If there is still interest in supporting Java 8 client after 2028, we would still be able to have extra
    releases for the 4.0 branch to address issues, security fixes. Although we need to be aware that it
    might be very hard to patch all vulnerabilities reported in dependencies at that point.

## Rejected alternatives

Technically, we could upgrade these dependencies and only require Java 17 for `pulsar-client-admin` and Java 8 for
`pulsar-client`. While this option might offer a wider compatibility today, it would introduce further confusion
on which Java is required for which component, which I don't believe is worth the effort.

# Links

 * [1] PIP-156 (Build and Run Pulsar Server on Java 17) https://github.com/apache/pulsar/issues/15207
 * [2] Mailing list discussion https://lists.apache.org/thread/cryoksz7n2066lzdcmhk9jy322lvh11t
 * [3] Java support and EOL timeline: https://endoflife.date/oracle-jdk
 * [4] NewRelic report on Java ecosystem https://newrelic.com/resources/report/2024-state-of-the-java-ecosystem