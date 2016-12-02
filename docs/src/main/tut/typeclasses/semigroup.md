---
layout: docs
title:  "Semigroup"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Semigroup.scala"
scaladoc: "#com.twitter.algebird.Semigroup"
---

# Semigroup

Given a set S and an operation `+`, we say that `(S, +)` is a *semigroup* if it satisfies the following properties for any x, y, z &isin; S:

- *Closure*: `x + y` &isin; S
- *Associativity*: `(x + y) + z = x + (y + z)`

We also say that *S forms a semigroup under +*.

## Examples of Semigroups

- Strings form a semigroup under concatenation.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/semigroup.md). These links might be helpful:

- [Semigroup.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Semigroup.scala)
- [SemigroupTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/SemigroupTest.scala)
