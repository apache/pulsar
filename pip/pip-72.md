# PIP-72: Introduce Pulsar Interface Taxonomy: Audience and Stability Classification

* **Status**: Accepted
* **Author**: Sijie Guo
* **Pull Request**: https://github.com/apache/pulsar/pull/8530
* **Mailing List discussion**:
* **Release**: 2.7.0

## Motivation

The whole Pulsar project has so many different interfaces. There was no clear way to define the audience and the stability of interfaces.

The proposal is to introduce an interface taxonomy classification, which is used to guide a developer to declare the targeted audience or users of an interface and also its stability.

- Benefits to the user of an interface: Knows which interfaces to use or not use and their stability.
- Benefits to the developer: to prevent accidental changes of interfaces and hence accidental impact on users or other components or systems. This is particularly useful in large systems with many developers who may not all have a shared state/history of the project.

## Interface Classification

This PIP proposes adopting the following interface classification, this classification was derived from the [BookKeeper taxonomy](https://bookkeeper.apache.org/docs/latest/api/javadoc/org/apache/bookkeeper/common/annotation/package-frame.html). Interfaces have two main attributes: Audience and Stability.

### Audience

Audience denotes the potential consumers of the interface. While many interfaces are internal/private to the implementation, others are public/external interfaces that are meant for wider consumption by applications and/or clients. For example, in POSIX, libc is an external or public interface, while large parts of the kernel are internal or private interfaces. Also, some interfaces are targeted towards other specific subsystems.

Identifying the audience of an interface helps define the impact of breaking it. For instance, it might be okay to break the compatibility of an interface whose audience is a small number of specific subsystems. On the other hand, it is probably not okay to break a protocol interface that millions of Internet users depend on.

Pulsar uses the following kinds of audience in order of increasing/wider visibility:

Pulsar doesn’t have a Company-Private classification, which is meant for APIs which are intended to be used by other projects within the company, since it doesn’t apply to opensource projects. Also, certain APIs are annotated as @VisibleForTesting (from com.google.common .annotations.VisibleForTesting) - these are meant to be used strictly for unit tests and should be treated as “Private” APIs.

#### Private

A Private interface is for internal use within the project (such as Functions or SQL) and should not be used by applications or by other projects. Most interfaces of a project are Private (also referred to as project-private). Unless an interface is intentionally exposed for external consumption, it should be marked Private.

#### Limited-Private

A Limited-Private interface is used by a specified set of projects or systems (typically closely related projects. for example, KoP, AoP, and etc). Other projects or systems should not use the interface. Changes to the interface will be communicated/negotiated with the specified projects.

#### Public

A Public interface is for general use by any application.

### Change Compatibility

Changes to an API fall into two broad categories: compatible and incompatible. A compatible change is a change that meets the following criteria:

- no existing capabilities are removed,
- no existing capabilities are modified in a way that prevents their use by clients that were constructed to use the interface prior to the change, and
- no capabilities are added that require changes to clients that were constructed to use the interface prior to the change.

Any change that does not meet these three criteria is an incompatible change. Stated simply a compatible change will not break existing clients. These examples are compatible changes:

- adding a method to a Java class,
- adding an optional parameter to a RESTful web service, or
- making the audience annotation of an interface broader (e.g. from Private to Public) or the change compatibility annotation more restrictive (e.g. from Evolving to Stable)

These examples are incompatible changes:

- removing a method from a Java class,
- adding a non-default method to a Java interface,
- adding a required parameter to a RESTful web service, or
- renaming a field in a JSON document.
- making the audience annotation of an interface less broad (e.g. from Public to Limited Private) or the change compatibility annotation more restrictive (e.g. from Evolving to Unstable)

### Stability

Stability denotes how stable an interface is and when compatible and incompatible changes to the interface are allowed. Pulsar APIs have the following levels of stability.

#### Stable

A Stable interface is exposed as a preferred means of communication. A Stable interface is expected not to change incompatibly within a major release and hence serves as a safe development target. A Stable interface may evolve compatibly between minor releases.

Incompatible changes allowed: major (X.0.0) Compatible changes allowed: maintenance (x.y.Z)

#### Evolving

An Evolving interface is typically exposed so that users or external code can make use of a feature before it has stabilized. The expectation that an interface should “eventually” stabilize and be promoted to Stable, however, is not a requirement for the interface to be labeled as Evolving.

Incompatible changes are allowed for the `Evolving` interface only at minor releases.

Incompatible changes allowed: minor (x.Y.0) Compatible changes allowed: maintenance (x.y.Z)

#### Unstable

An Unstable interface is one for which no compatibility guarantees are made. An Unstable interface is not necessarily unstable. An unstable interface is typically exposed because a user or external code needs to access an interface that is not intended for consumption. The interface is exposed as an Unstable interface to state clearly that even though the interface is exposed, it is not the preferred access path, and no compatibility guarantees are made for it.

Unless there is a reason to offer a compatibility guarantee on an interface, whether it is exposed or not, it should be labeled as Unstable. Private interfaces also should be Unstable in most cases.

Incompatible changes to Unstable interfaces are allowed at any time.

Incompatible changes allowed: maintenance (x.y.Z) Compatible changes allowed: maintenance (x.y.Z)

#### Deprecated

A Deprecated interface could potentially be removed in the future and should not be used. Even so, a Deprecated interface will continue to function until it is removed. When a Deprecated interface can be removed depends on whether it is also Stable, Evolving, or Unstable.

### How are the Classifications Recorded?

How will the classification be recorded for Pulsar APIs?

- Each interface or class will have the audience and stability recorded using annotations in the org.apache.pulsar.common.classification package.
- The javadoc generated by the maven target javadoc:javadoc lists only the public API.
- One can derive the audience of java classes and java interfaces by the audience of the package in which they are contained. Hence it is useful to declare the audience of each java package as public or private (along with the private audience variations).

How will the classification be recorded for other interfaces, such as CLIs?

- We will introduce a separate PIP for capturing the compatibility goals for other components in Pulsar.

## FAQ

- Why aren’t the java scopes (private, package private and public) good enough?
  - Java’s scoping is not very complete. One is often forced to make a class public in order for other internal components to use it. It also does not have friends or sub-package-private like C++.

- But I can easily access a Private interface if it is Java public. Where is the protection and control?
  - The purpose of this classification scheme is not providing absolute access control. Its purpose is to communicate to users and developers. One can access private implementation functions in libc; however if they change the internal implementation details, the application will break and one will receive little sympathy from the folks who are supplying libc. When using a non-public interface, the risks are understood.

- Why bother declaring the stability of a Private interface? Aren’t Private interfaces always Unstable?
  - Private interfaces are not always Unstable. In the cases where they are Stable they capture internal properties of the system and can communicate these properties to its internal users and to developers of the interface.

- What is the harm in applications using a Private interface that is Stable? How is it different from a Public Stable interface?
  - While a Private interface marked as Stable is targeted to change only at major releases, it may break at other times if the providers of that interface also are willing to change the internal consumers of that interface. Further, a Public Stable interface is less likely to break even at major releases (even though it is allowed to break compatibility) because the impact of the change is larger. If you use a Private interface (regardless of its stability) you run the risk of incompatibility.

- Why bother with Limited-Private? Isn’t it giving special treatment to some projects? That is not fair.
  - Most interfaces should be Public or Private. An interface should be Private unless it is explicitly intended for general use.
  - Limited-Private is for interfaces that are not intended for general use. They are exposed to related projects that need special hooks. Such a classification has a cost to both the supplier and consumer of the interface. Both will have to work together if ever there is a need to break the interface in the future; for example the supplier and the consumers will have to work together to get coordinated releases of their respective projects. This contract should not be taken lightly–use Private if possible; if the interface is really for general use for all applications then use Public. Always remember that making an interface Public comes with large burden of responsibility. Sometimes Limited-Private is just right.
  - If you have a Limited-Private interface with many projects listed then the interface is probably a good candidate to be made Public.

- Aren’t all Public interfaces Stable?
  - One may mark a Public interface as Evolving in its early days. Here one is promising to make an effort to make compatible changes but may need to break it at minor releases.
  - One example of a Public interface that is Unstable is where one is providing an implementation of a standards-body based interface that is still under development. For example, many companies, in an attempt to be first to market, have provided implementations of a new NFS protocol even when the protocol was not fully completed by IETF. The implementor cannot evolve the interface in a fashion that causes least disruption because the stability is controlled by the standards body. Hence it is appropriate to label the interface as Unstable.
