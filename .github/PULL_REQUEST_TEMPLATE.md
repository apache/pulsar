<!--
### Contribution Checklist
  
  - PR title format should be *[type][component] summary*. For details, see *[Guideline - Pulsar PR Naming Convention](https://pulsar.apache.org/contribute/develop-semantic-title/)*. 

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.
  
  - Each pull request should address only one issue, not mix up code from multiple issues.
  
  - Each commit in the pull request has a meaningful commit message

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.
-->

<!-- Either this PR fixes an issue, -->

Fixes #xyz

<!-- or this PR is one task of an issue -->

Main Issue: #xyz

<!-- If the PR belongs to a PIP, please add the PIP link here -->

PIP: #xyz 

<!-- Details of when a PIP is required and how the PIP process work, please see: https://github.com/apache/pulsar/blob/master/pip/README.md -->

### Motivation

<!-- Explain here the context, and why you're making that change. What is the problem you're trying to solve. -->

### Modifications

<!-- Describe the modifications you've done. -->

### Verifying this change

- [ ] Make sure that the change passes the CI checks.

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *Added integration tests for end-to-end deployment with large payloads (10MB)*
  - *Extended integration test for recovery after broker failure*

### Does this pull request potentially affect one of the following parts:

<!-- DO NOT REMOVE THIS SECTION. CHECK THE PROPER BOX ONLY. -->

*If the box was checked, please highlight the changes*

- [ ] Dependencies (add or upgrade a dependency)
- [ ] The public API
- [ ] The schema
- [ ] The default values of configurations
- [ ] The threading model
- [ ] The binary protocol
- [ ] The REST endpoints
- [ ] The admin CLI options
- [ ] The metrics
- [ ] Anything that affects deployment
