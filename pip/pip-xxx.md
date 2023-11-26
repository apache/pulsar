# PIP-XXX OpenTelemetry Scaffolding 

# Background knowledge

## PIP-264 - parent PIP titled "Enhanced OTel-based metric system"
[PIP-264](https://github.com/apache/pulsar/pull/21080), which can also be viewed [here](pip-264.md), describes in high 
level a plan to greatly enhance Pulsar metric system by replacing it with [OpenTelemetry](https://opentelemetry.io/).
You can read in the PIP the numerous existing problems PIP-264 solves. Among them are:
- Control which metrics to export per topic/group/namespace via the introduction of a metric filter configuration
- Reduce the immense metrics cardinality due to high topic count (One of Pulsar great features), by introducing
the concept of Metric Group - a group of topics for metric purposes. Metric reporting will also be done to a 
group granularity. 100k topics can be downsized to 1k groups. The dynamic metric filter configuration would allow 
the user to control which metric group to un-filter. 
- Proper histogram exporting
- Clean-up codebase clutter, by relying on a single industry standard API, SDK and metrics protocol (OTLP) instead of 
existing mix of home-brew libraries and hard coded Prometheus exporter.
- any many more

You can [here](pip-264.md#why-opentelemetry) why OpenTelemetry was chosen.

## OpenTelemetry
Since OpenTelemetry (a.k.a. OTel) is an emerging industry standard, there are plenty of good articles, videos and
documentation about it. In this very short paragraph I'll describe what you need to know about OTel from this PIP
perspective.

OpenTelemetry is a project aimed to standardize the way we instrument, collect and ship metrics from applications
to telemetry backends, be it databases (e.g. Prometheus, Cortex, Thanos) or vendors (e.g. Datadog, Logz.io).
It is divided into API, SDK and Collector:
- API: interfaces to use to instrument: define a counter, record values to a histogram, etc.
- SDK: a library, available in many languages, implementing the API, and other important features such as
reading the metrics and exporting it out to a telemetry backend or OTel Collector. 
- Collector: a lightweight process (application) which can receive or retrieve telemetry, transform it (e.g.
filter, drop, aggregate)  and export it (e.g. send it to various backends). The SDK supports out-of-the-box 
exporting metrics as Prometheus HTTP endpoint or sending them out using OTLP protocol. Many times companies choose to
ship to the Collector and there ship to their preferred vendors, since each vendor already published their exporter
plugin to OTel Collector. This makes the SDK exporters very light-weight as they don't need to support any 
vendor. It's also easier for the DevOps team as they can make OTel Collector their responsibility, and have
application developers only focus on shipping metrics to that collector.

Just to have some context: Pulsar codebase will use the OTel API to create counters / histograms and records values to 
them. So will the Pulsar plugins and Pulsar Function authors. Pulsar itself will be the one creating the SDK
and using that to hand over an implementation of the API where ever needed in Pulsar. Collector is up to the choice
of the user, as OTel provides a way to expose the metrics as `/metrics` endpoint on a configured port, so Prometheus
compatible scrapers can grab it from it directly. They can also send it via OTLP to OTel collector.


# Motivation



<!--
Describe the problem this proposal is trying to solve.

* Explain what is the problem you're trying to solve - current situation.
* This section is the "Why" of your proposal.
-->

# Goals

## In Scope

<!--
What this PIP intend to achieve once It's integrated into Pulsar.
Why does it benefit Pulsar.
-->

## Out of Scope

<!--
Describe what you have decided to keep out of scope, perhaps left for a different PIP/s.
-->


# High Level Design

<!--
Describe the design of your solution in *high level*.
Describe the solution end to end, from a birds-eye view.
Don't go into implementation details in this section.

I should be able to finish reading from beginning of the PIP to here (including) and understand the feature and 
how you intend to solve it, end to end.

DON'T
* Avoid code snippets, unless it's essential to explain your intent.
-->

# Detailed Design

## Design & Implementation Details

<!--
This is the section where you dive into the details. It can be:
* Concrete class names and their roles and responsibility, including methods.
* Code snippets of existing code.
* Interface names and its methods.
* ...
-->

## Public-facing Changes

<!--
Describe the additions you plan to make for each public facing component. 
Remove the sections you are not changing.
Clearly mark any changes which are BREAKING backward compatability.
-->

### Public API
<!--
When adding a new endpoint to the REST API, please make sure to document the following:

* path
* query parameters
* HTTP body parameters, usually as JSON.
* Response codes, and for each what they mean.
  For each response code, please include a detailed description of the response body JSON, specifying each field and what it means.
  This is the place to document the errors.
-->

### Binary protocol

### Configuration

### CLI

### Metrics

<!--
For each metric provide:
* Full name
* Description
* Attributes (labels)
* Unit
-->


# Monitoring

<!-- 
Describe how the changes you make in this proposal should be monitored. 
Don't describe the detailed metrics - they should be at "Public-facing Changes" / "Metrics" section.
Describe how the user will use the metrics to monitor the feature: Which alerts they should set up, which thresholds, ...
-->

# Security Considerations
<!--
A detailed description of the security details that ought to be considered for the PIP. This is most relevant for any new HTTP endpoints, new Pulsar Protocol Commands, and new security features. The goal is to describe details like which role will have permission to perform an action.

An important aspect to consider is also multi-tenancy: Does the feature I'm adding have the permissions / roles set in such a way that prevent one tenant accessing another tenant's data/configuration? For example, the Admin API to read a specific message for a topic only allows a client to read messages for the target topic. However, that was not always the case. CVE-2021-41571 (https://github.com/apache/pulsar/wiki/CVE-2021-41571) resulted because the API was incorrectly written and did not properly prevent a client from reading another topic's messages even though authorization was in place. The problem was missing input validation that verified the requested message was actually a message for that topic. The fix to CVE-2021-41571 was input validation. 

If there is uncertainty for this section, please submit the PIP and request for feedback on the mailing list.
-->

# Backward & Forward Compatibility

## Revert

<!--
Describe a cookbook detailing the steps required to revert pulsar to previous version *without* this feature.
-->

## Upgrade

<!--
Specify the list of instructions, if there are such, needed to perform before/after upgrading to Pulsar version containing this feature.
-->

# Alternatives

<!--
If there are alternatives that were already considered by the authors or, after the discussion, by the community, and were rejected, please list them here along with the reason why they were rejected.
-->

# General Notes

# Links

<!--
Updated afterwards
-->
* Mailing List discussion thread:
* Mailing List voting thread:
