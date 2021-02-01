---
layout: docs
title:  "Monoid"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Monoid.scala"
scaladoc: "#com.twitter.algebird.Monoid"
---

# Monoid

A monoid is a semigroup with an identity element.  More formally, given a set M and an operation `+`, we say that `(M, +)` is a *monoid* if it satisfies the following properties for any x, y, z &isin; M:

- *Closure*: `x + y` &isin; M
- *Associativity*: `(x + y) + z = x + (y + z)`
- *Identity*: There exists an e &isin; M such that `e + x = x + e = x`

We also say that *M is a monoid under +.*

## Examples of Monoids

- The natural numbers *N* are monoids under addition
- *N* is a monoid under multiplication.
- Strings form a monoid under concatenation (`""` is the identity element)

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/monoid.md). These links might be helpful:

- [Monoid.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Monoid.scala)
