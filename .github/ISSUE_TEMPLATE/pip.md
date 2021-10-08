---
name: PIP request
about: Submit a PIP (Pulsar Improvement Proposal)
title: ''
labels: PIP
assignees: ''

---
<!---
Instructions for creating a PIP using this issue template:

 1. The author(s) of the proposal will create a GitHub issue ticket using this template.
    (Optionally, it can be helpful to send a note discussing the proposal to
    dev@pulsar.apache.org mailing list before submitting this GitHub issue. This discussion can
    help developers gauge interest in the proposed changes before formalizing the proposal.)
 2. The author(s) will send a note to the dev@pulsar.apache.org mailing list
    to start the discussion, using subject prefix `[PIP] xxx`. To determine the appropriate PIP
    number `xxx`, inspect the mailing list (https://lists.apache.org/list.html?dev@pulsar.apache.org)
    for the most recent PIP. Add 1 to that PIP's number to get your PIP's number.
 3. Based on the discussion and feedback, some changes might be applied by
    the author(s) to the text of the proposal.
 4. Once some consensus is reached, there will be a vote to formally approve
    the proposal. The vote will be held on the dev@pulsar.apache.org mailing list. Everyone
    is welcome to vote on the proposal, though it will considered to be binding
    only the vote of PMC members. It will be required to have a lazy majority of
    at least 3 binding +1s votes. The vote should stay open for at least 48 hours.
 5. When the vote is closed, if the outcome is positive, the state of the
    proposal is updated and the Pull Requests associated with this proposal can
    start to get merged into the master branch.

-->

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
