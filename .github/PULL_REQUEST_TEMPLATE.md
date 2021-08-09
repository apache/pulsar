<!--
### Contribution Checklist
  
  - Name the pull request in the form "[Issue XYZ][component] Title of the pull request", where *XYZ* should be replaced by the actual issue number.
    Skip *Issue XYZ* if there is no associated github issue for this pull request.
    Skip *component* if you are unsure about which is the best component. E.g. `[docs] Fix typo in produce method`.

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

#### For contributor

For this PR, do we need to update docs?

- If yes, please update docs or create a follow-up issue if you need help.
  
- If no, please explain why.

#### For committer

For this PR, do we need to update docs?

- If yes,
  
  - if you update docs in this PR, label this PR with the `doc` label.
  
  - if you plan to update docs later, label this PR with the `doc-required` label.

  - if you need help on updating docs, create a follow-up issue with the `doc-required` label.
  
- If no, label this PR with the `no-need-doc` label and explain why.

