# Pulsar Improvement Proposal (PIP)

## What is a PIP?

The PIP is a "Pulsar Improvement Proposal" and it's the mechanism used to propose changes to the Apache Pulsar codebases.

The changes might be in terms of new features, large code refactoring, changes to APIs.

In practical terms, the PIP defines a process in which developers can submit a design doc, receive feedback and get the "go ahead" to execute.

### What is the goal of a PIP?

There are several goals for the PIP process:

1. Ensure community technical discussion of major changes to the Apache Pulsar codebase.

2. Provide clear and thorough design documentation of the proposed changes. Make sure every Pulsar developer will have enough context to effectively perform a code review of the Pull Requests.

3. Use the PIP document to serve as the baseline on which to create the documentation for the new feature.

4. Have greater scrutiny to changes are affecting the public APIs (as defined below) to reduce chances of introducing breaking changes or APIs that are not expressing an ideal semantic.

It is not a goal for PIP to add undue process or slow-down the development.

### When is a PIP required?

* Any new feature for Pulsar brokers or client
* Any change to the public APIs (Client APIs, REST APIs, Plugin APIs)
* Any change to the wire protocol APIs
* Any change to the API of Pulsar CLI tools (eg: new options)
* Any change to the semantic of existing functionality, even when current behavior is incorrect.
* Any large code change that will touch multiple components
* Any changes to the metrics (metrics endpoint, topic stats, topics internal stats, broker stats, etc.)
* Any change to the configuration

### When is a PIP *not* required?

* Bug-fixes
* Simple enhancements that won't affect the APIs or the semantic
* Small documentation changes
* Small website changes
* Build scripts changes (except: a complete rewrite)

### Who can create a PIP?

Any person willing to contribute to the Apache Pulsar project is welcome to create a PIP.

## How does the PIP process work?

A PIP proposal can be in these states:
1. **DRAFT**: (Optional) This might be used for contributors to collaborate and to seek feedback on an incomplete version of the proposal.

2. **DISCUSSION**: The proposal has been submitted to the community for feedback and approval.

3. **ACCEPTED**: The proposal has been accepted by the Pulsar project.

4. **REJECTED**: The proposal has not been accepted by the Pulsar project.

5. **IMPLEMENTED**: The implementation of the proposed changes have been completed and everything has been merged.

6. **RELEASED**: The proposed changes have been included in an official
   Apache Pulsar release.


The process works in the following way:

1. Fork https://github.com/apache/pulsar repository (Using the fork button on GitHub).
2. Clone the repository, and on it, copy the file `pip/TEMPLATE.md` and name it `pip-xxx.md`. The number `xxx` should be the next sequential number after the last contributed PIP. You view the list of contributed PIPs (at any status) as a list of Pull Requests having a title starting with `[pip][design] PIP-`. Use the link [here](https://github.com/apache/pulsar/pulls?q=is%3Apr+title%3A%22%5Bpip%5D%5Bdesign%5D+PIP-%22) as shortcut.
3. Write the proposal following the section outlined by the template and the explanation for each section in the comment it contains (you can delete the comment once done).
   * If you need diagrams, avoid attaching large files. You can use [MermaidJS](https://mermaid.js.org/) as simple language to describe many types of diagrams. 
4. Create GitHub Pull request (PR). The PR title should be `[pip][design] PIP-xxx: {title}`, where the `xxx` match the number given in previous step (file-name). Replace `{title}` with a short title to your proposal.
5. The author(s) will email the dev@pulsar.apache.org mailing list to kick off a discussion, using subject prefix `[DISCUSS] PIP-xxx: {PIP TITLE}`. The discussion will happen in broader context either on the mailing list or as general comments on the PR. Many of the discussion items will be on particular aspect of the proposal, hence they should be as comments in the PR to specific lines in the proposal file.
6. Update file with a link to the discussion on the mailing. You can obtain it from [Apache Pony Mail](https://lists.apache.org/list.html?dev@pulsar.apache.org).
7. Based on the discussion and feedback, some changes might be applied by authors to the text of the proposal. They will be applied as extra commits, making it easier to track the changes.
8. Once some consensus is reached, there will be a vote to formally approve the proposal. The vote will be held on the dev@pulsar.apache.org mailing list, by
   sending a message using subject `[VOTE] PIP-xxx: {PIP TITLE}`.
   Make sure to update the PIP with a link to the vote. You can obtain it from [Apache Pony Mail](https://lists.apache.org/list.html?dev@pulsar.apache.org). 
   Everyone is welcome to vote on the proposal, though only the vote of the PMC members will be considered binding.
   It is required to have a lazy majority of at least 3 binding +1s votes.
   The vote should stay open for at least 48 hours.
9. When the vote is closed, if the outcome is positive, ask a PMC member (using voting thread on mailing list) to merge the PR.
10. If the outcome is negative, please close the PR (with a small comment that the close is a result of a vote).

All the future implementation Pull Requests that will be created, should always reference the PIP-XXX in the commit log message and the PR title.
It is advised to create a master GitHub issue to formulate the execution plan and track its progress.

## List of PIPs

### Historical PIPs
You can the view list of PIPs previously managed by GitHub wiki or GitHub issues [here](https://github.com/apache/pulsar/wiki#pulsar-improvement-proposals)

### List of PIPs
1. You can view all PIPs (besides the historical ones) as the list of Pull Requests having title starting with `[pip][design] PIP-`. Here is the [link](https://github.com/apache/pulsar/pulls?q=is%3Apr+title%3A%22%5Bpip%5D%5Bdesign%5D+PIP-%22) for it. 
   - Merged PR means the PIP was accepted.
   - Closed PR means the PIP was rejected.
   - Open PR means the PIP was submitted and is in the process of discussion.
2. You can also take a look at the file in the `pip` folder. Each one is an approved PIP.