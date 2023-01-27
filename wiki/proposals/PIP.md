# Pulsar Improvement Proposal (PIP)

## What is a PIP?

The PIP is a "Pulsar Improvement Proposal" and it's the mechanism used to
propose changes to the Apache Pulsar codebases.

The changes might be in terms of new features, large code refactoring, changes
to APIs.

In practical terms, the PIP defines a process in which developers can submit
a design doc, receive feedback and get the "go ahead" to execute.

## What is the goal of a PIP?

There are several goals for the PIP process:

1. Ensure community technical discussion of major changes to the Apache Pulsar
   codebase

2. Provide clear and thorough design documentation of the proposed changes.
   Make sure every Pulsar developer will have enough context to effectively
   perform a code review of the Pull Requests.

3. Use the PIP document to serve as the starting base on which to create the
   documentation for the new feature.

4. Have greater scrutiny to changes are affecting the public APIs to reduce
   chances of introducing breaking changes or APIs that are not expressing
   an ideal semantic.


It is not a goal for PIP to add undue process or slow-down the development.

## When is a PIP required?

* Any new feature for Pulsar brokers or client
* Any change to the public APIs (Client APIs, REST APIs, Plugin APIs)
* Any change to the wire protocol APIs
* Any change to the API of Pulsar CLI tools (eg: new options)
* Any change to the semantic of existing functionality, even when current
  behavior is incorrect.
* Any large code change that will touch multiple components
* Any changes to the metrics (metrics endpoint, topic stats, topics internal stats, broker stats, etc.)

## When is a PIP *not* required?

* Bug-fixes
* Simple enhancements that won't affect the APIs or the semantic
* Documentation changes
* Website changes
* Build scripts changes (except: a complete rewrite)

## Who can create a PIP?

Any person willing to contribute to the Apache Pulsar project is welcome to
create a PIP.

## How does the PIP process work?

A PIP proposal can be in these states:
1. **DRAFT**: (Optional) This might be used for contributors to collaborate and
   to seek feedback on an incomplete version of the proposal.

2. **DISCUSSION**: The proposal has been submitted to the community for
   feedback and approval.

3. **ACCEPTED**: The proposal has been accepted by the Pulsar project.

4. **REJECTED**: The proposal has not been accepted by the Pulsar project.

5. **IMPLEMENTED**: The implementation of the proposed changes have been
   completed and everything has been merged.

5. **RELEASED**: The proposed changes have been included in an official
   Apache Pulsar release.

The process works in the following way:

1. The author(s) of the proposal will create a GitHub issue ticket choosing the
   template for PIP proposals.
2. The author(s) will send a note to the dev@pulsar.apache.org mailing list
   to start the discussion, using subject prefix `[PIP] xxx`. The discussion
   need to happen in the mailing list. Please avoid discussing it using
   GitHub comments in the PIP GitHub issue, as it creates two tracks 
   of feedback.
3. Based on the discussion and feedback, some changes might be applied by
   authors to the text of the proposal.
4. Once some consensus is reached, there will be a vote to formally approve
   the proposal.
   The vote will be held on the dev@pulsar.apache.org mailing list. Everyone
   is welcome to vote on the proposal, though it will considered to be binding
   only the vote of PMC members.
   I would be required to have a lazy majority of at least 3 binding +1s votes.
   The vote should stay open for at least 48 hours.
5. When the vote is closed, if the outcome is positive, the state of the
   proposal is updated and the Pull Requests associated with this proposal can
   start to get merged into the master branch.

All the Pull Requests that are created, should always reference the
PIP-XXX in the
commit log message and the PR title.

## Labels of a PIP

In addition to its current state, the GitHub issue for the PIP will also be
tagged with other labels.

Some examples:
* Execution status: In progress, Completed, Need Help, ...
* Targeted Pulsar release: 2.9, 2.10, ...


## Template for a PIP design doc

```
## Motivation

Explain why this change is needed, what benefits it would bring to Apache Pulsar
and what problem it's trying to solve.

## Goal

Define the scope of this proposal. Given the motivation stated above, what are
the problems that this proposal is addressing and what other items will be
considering out of scope, perhaps to be left to a different PIP.

## API Changes

Illustrate all the proposed changes to the API or wire protocol, with examples
of all the newly added classes/methods, including Javadoc.

## Implementation

This should be a detailed description of all the changes that are
expected to be made. It should be detailed enough that any developer that is
familiar with Pulsar internals would be able to understand all the parts of the
code changes for this proposal.

This should also serve as documentation for any person that is trying to
understand or debug the behavior of a certain feature.


## Reject Alternatives

If there are alternatives that were already considered by the authors or,
after the discussion, by the community, and were rejected, please list them
here along with the reason why they were rejected.

```