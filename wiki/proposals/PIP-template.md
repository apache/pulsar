# Pulsar Improvement Proposal Template

* **Status**: "one of ['Under Discussion', 'Accepted', 'Adopted', 'Rejected']"
* **Author**: (Names)
* **Pull Request**: (Link to the main pull request to resolve this PIP)
* **Mailing List discussion**: (Link to the mailing list discussion)
* **Release**: (Which release include this PIP)

### Motivation

_Describe the problems you are trying to solve_

### Public Interfaces

_Briefly list any new interfaces that will be introduced as part of this proposal or any existing interfaces that will be removed or changed. The purpose of this section is to concisely call out the public contract that will come along with this feature._

A public interface is any change to the following:

- Data format, Metadata format
- The wire protocol and API behavior
- Any class in the public packages
- Monitoring
- Command-line tools and arguments
- Configuration settings
- Anything else that will likely break existing users in some way when they upgrade

### Proposed Changes

_Describe the new thing you want to do in appropriate detail. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences. Use judgment based on the scope of the change._

### Compatibility, Deprecation, and Migration Plan

- What impact (if any) will there be on existing users?
- If we are changing behavior how will we phase out the older behavior?
- If we need special migration tools, describe them here.
- When will we remove the existing behavior?

### Test Plan

_Describe in few sentences how the BP will be tested. We are mostly interested in system tests (since unit-tests are specific to implementation details). How will we know that the implementation works as expected? How will we know nothing broke?_

### Rejected Alternatives

_If there are alternative ways of accomplishing the same thing, what were they? The purpose of this section is to motivate why the design is the way it is and not some other way._

---

```
* **Status**: "one of ['Under Discussion', 'Accepted', 'Adopted', 'Rejected']"
* **Author**: (Names)
* **Pull Request**: (Link to the main pull request to resolve this PIP)
* **Mailing List discussion**: (Link to the mailing list discussion)
* **Release**: (Which release include this PIP)

### Motivation

_Describe the problems you are trying to solve_

### Public Interfaces

_Briefly list any new interfaces that will be introduced as part of this proposal or any existing interfaces that will be removed or changed. The purpose of this section is to concisely call out the public contract that will come along with this feature._

A public interface is any change to the following:

- Data format, Metadata format
- The wire protocol and API behavior
- Any class in the public packages
- Monitoring
- Command-line tools and arguments
- Configuration settings
- Anything else that will likely break existing users in some way when they upgrade

### Proposed Changes

_Describe the new thing you want to do in appropriate detail. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences. Use judgment based on the scope of the change._

### Compatibility, Deprecation, and Migration Plan

- What impact (if any) will there be on existing users? 
- If we are changing behavior how will we phase out the older behavior? 
- If we need special migration tools, describe them here.
- When will we remove the existing behavior?

### Test Plan

_Describe in few sentences how the BP will be tested. We are mostly interested in system tests (since unit-tests are specific to implementation details). How will we know that the implementation works as expected? How will we know nothing broke?_

### Rejected Alternatives

_If there are alternative ways of accomplishing the same thing, what were they? The purpose of this section is to motivate why the design is the way it is and not some other way._
```