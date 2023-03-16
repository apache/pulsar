---
name: PIP
about: Submit a Pulsar Improvement Proposal (PIP)
title: 'PIP-XYZ: '
labels: PIP
---

<!--
RULES
* Never place a link to an external site like Google Doc. The proposal should be in this issue entirely.
* Use a spelling and grammar checker tools if available for you (there are plenty of free ones)

PROPOSAL HEALTH CHECK
I can read the design document and understand the problem statement and what you plan to change *without* resorting to a couple of hours of code reading just to start having a high level understanding of the change.
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
* See PIP-248, Background section to get an understanding on how you add the background knowledge needed.
  (They also included the motivation there, but ignore it as we place that in Motivation section explicitly)
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
Why does it benefit Pulsar
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
* Concrete class names and their roles and responsibility, including methods
* Code snippets of existing code 
* interface names and its methods,
* ...
-->

## Public-facing Changes

<!--
Describe the additions you plan to make for each public facing component. 
Remove the sections you are not changing.
Clearly mark any changes which are BREAKING backward compatability.
-->

### Public API 

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
Don't describe the detailed metrics - they should be at "Public-facing Changes" / "Metrics" section
Describe how the user will use the metrics to monitor the feature: Which alerts they should set up, which thresholds, ...
-->

# Security Considerations
<!--
A detailed description of the security details that ought to be considered for the PIP. This is most relevant for any new HTTP endpoints, new Pulsar Protocol Commands, and new security features. The goal is to describe details like which role will have permission to perform an action.

If there is uncertainty for this section, please submit the PIP and request for feedback on the mailing list.
-->

# Backward Compatability

## Revert

<!--
Describe a cookbook detailing the steps required to revert pulsar to previous version *without* this feature.
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
