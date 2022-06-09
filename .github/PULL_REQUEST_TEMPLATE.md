<!--
### Contribution Checklist
  
  - PR title format should be *[type][component] summary*. For details, see *[Guideline - Pulsar PR Naming Convention](https://docs.google.com/document/d/1d8Pw6ZbWk-_pCKdOmdvx9rnhPiyuxwq60_TrD68d7BA/edit#heading=h.trs9rsex3xom)*. 

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.
  
  - Each pull request should address only one issue, not mix up code from multiple issues.
  
  - Each commit in the pull request has a meaningful commit message

  - Once all items of the checklist are addressed, remove the above text and this checklist, leaving only the filled out template below.

**(The sections below can be removed for hotfixes of typos)**
-->

*(If this PR fixes a github issue, please add `Fixes #<xyz>`.)*

Fixes #<xyz>

*(or if this PR is one task of a github issue, please add `Master Issue: #<xyz>` to link to the master issue.)*

Master Issue: #<xyz>

### Motivation


*Explain here the context, and why you're making that change. What is the problem you're trying to solve.*

### Modifications

*Describe the modifications you've done.*

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

*If `yes` was chosen, please highlight the changes*

  - Dependencies (does it add or upgrade a dependency): (yes / no)
  - The public API: (yes / no)
  - The schema: (yes / no / don't know)
  - The default values of configurations: (yes / no)
  - The wire protocol: (yes / no)
  - The rest endpoints: (yes / no)
  - The admin cli options: (yes / no)
  - Anything that affects deployment: (yes / no / don't know)

### Documentation

Check the box below or label this PR directly.

Need to update docs? 

- [ ] `doc-required` 
(Your PR needs to update docs and you will update later)
  
- [ ] `doc-not-needed` 
(Please explain why)
  
- [ ] `doc` 
(Your PR contains doc changes)

- [ ] `doc-complete`
(Docs have been already added)