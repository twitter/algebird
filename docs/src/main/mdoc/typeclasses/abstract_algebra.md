---
layout: docs
title:  "Abstract Algebra"
section: "typeclasses"
---

# Abstract Algebra

Algebird is a library of data structures with interesting algebraic properties.

Here are the algebraic structures we support:

- [`Semigroup`](semigroup.html)
- [`Monoid`](monoid.html)
- [`Group`](group.html)
- [`Ring`](ring.html)
- [`Metric`](metric.html)

## Language

TODO: Comment on "is a monoid", "forms a monoid", "has a monoid" confusion.

## Syntax

Algebird's [operators](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Operators.scala) allow you to use `*`, `-` `+` on any type with a defined `Group`, `Ring` and `Semigroup`, respectively.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/abstract_algebra.md).
