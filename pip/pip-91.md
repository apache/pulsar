# PIP-91: Separate lookup timeout from operation timeout

* **Status:** 'Under Discussion'
* **Author:** Ivan Kelly (ivank@apache.org)
* **Pull Request:**
  * https://github.com/apache/pulsar/pull/11627
* **Mailing List discussion:** http://mail-archives.apache.org/mod_mbox/pulsar-dev/202108.mbox/browser
* **Release:** -

# Motivation

When connecting to a topic, the client needs to do a lookup to find the broker which owns the topic. This consists of two requests, a partition metadata request and a lookup request. Both of these requests can be served by any broker in the cluster.

Retries and timeouts are handled inconsistently for these two requests.

For partition metadata request (PMR henceforth), if the request times out (which happens after the configured operation timeout, 30 seconds by default), it will be retried up until operation timeout has passed. Which means it should not retry except for the fact that there is a bug which only counts the backoff time against the timeout, not the time waiting on timeout, so it does actually retry on timeout. PMR explicitly does not retry on TooManyRequests response.

Lookup requests timeouts are handled similarly. There is retry logic there, but the maximum amount of time for the lookup is the same as the actual operation timeout time, so there is effectively no retries on timeout. For lookups there is a retry on TooManyRequests.

PMR and lookup are different to produce and consume requests. They can be handled by any broker as stated earlier. Their frequency is not proportional to the traffic in the system, but to movement of topics around the cluster. For example, if there is a rolling restart of brokers, all clients will have to lookup all topics again though the number of the messages moving through the system remains constant.

PMR and lookups are side effect free. Multiple requests for the same topic do not change the state of the system. 

For the above reason, PMR and Lookup should be more resilient to transient errors than other requests. To achieve this, I propose:
- Create a separate lookup timeout, which is a multiple of operation timeout.
- Make TooManyRequests retriable for both PMR and lookup
- Make binary proto lookup use a separate connection pool, and close the connection on error (configurable).

The final proposal needs a bit more explanation. In the case that clients are configured to connect to Pulsar via a loadbalancer, they will have a single IP endpoint which is used for all lookups. While the lookups should be evenly distributed among backends, when transient errors occur the retry will go to the exact same backend (as the connection is persistent) and the one that had the problem in the first place. Closing and reestablishing the connection will allow a new backend to be selected.

# Public Interfaces

- New configuration settings for the java client
  - lookup timeout (default to match operation timeout, so the matches current behaviour)
  - separate lookup connection pool

As part of these changes I will fix the bug in PMR timeout retries. This could impact users who have been experiencing transient errors previously that have been masked by the bug. For them the solution would be to explicitly set lookup timeout to a higher value.

# Proposed Changes

The changes proposed are as follows:

Add a new configuration for lookup timeout. This timeout will be used in place of operation timeout in the Backoff objects used by PMR and lookup to set the max amount of time retries can occur. The default value for this configuration will match operation timeout, so that behaviour will not change by default.

Fix the bug in timeout calculation in PMR. This may result in more PMR request timeouts reaching the client, as the overall timeout time is reduced.

Add a new configuration to use a separate connection pool for lookups. This only affects binary protocol lookups. If a separate pool is used, when a lookup request fails, the connection it is associated with is closed.

# Compatibility, Deprecation, and Migration Plan

There is a possible increase in timeout exceptions for some users due to the fix in timeout calculation.

# Test Plan

The change will be unit tested. New mocks will be added to allows us to easily trigger certain behaviours (timeouts, TooManyRequests) from ServerCnx.
