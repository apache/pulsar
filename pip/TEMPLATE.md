<!--
RULES
* Never place a link to an external site like Google Doc. The proposal should be in this issue entirely.
* Use a spelling and grammar checker tools if available for you (there are plenty of free ones).

PROPOSAL HEALTH CHECK
I can read the design document and understand the problem statement and what you plan to change *without* resorting to a couple of hours of code reading just to start having a high level understanding of the change.

THIS COMMENTS
Please remove them when done.
-->

# Background knowledge

<!--
Describes all the knowledge you need to know in order to understand all the other sections in this PIP

* Give a high level explanation on all concepts you will be using throughout this document. For example, if you want to talk about Persistent Subscriptions, explain briefly (1 paragraph) what this is. If you're going to talk about Transaction Buffer, explain briefly what this is. 
  If you're going to change something specific, then go into more detail about it and how it works. 
* Provide links where possible if a person wants to dig deeper into the background information. 

DON'T
* Do not include links *instead* explanation. Do provide links for further explanation.

EXAMPLES
* See [PIP-248](https://github.com/apache/pulsar/issues/19601), Background section to get an understanding on how you add the background knowledge needed.
  (They also included the motivation there, but ignore it as we place that in Motivation section explicitly).
-->

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

# Backward & Forward Compatability

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
