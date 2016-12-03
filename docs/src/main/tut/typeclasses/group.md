---
layout: docs
title:  "Group"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Group.scala"
scaladoc: "#com.twitter.algebird.Group"
---

# Group

We say that `(G, *)` is a *group* if it is a Monoid that also satisfies the following property:

- *Invertibility*: For every x &isin; G there is an xinv such that `x * xinv = xinv * x = e`

Moreover, it is an *abelian group* if it satisfies the property:

- *Commutative*: `x * y = y * x` for all x and y &isin; G.

## Examples of Groups

- Integers *Z* are an abelian group under addition
- Natural numbers are *not* a group under addition (given a number *x* in *N*, *-x* is not in *N*)
- Neither integers nor natural numbers are a group under multiplication, but the set of nonzero rational numbers (`n/d` for any `n, d` &isin; `N`, `n` &ne; 0, `d` &ne; 0) is an (abelian) group under multiplication.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/group.md). These links might be helpful:

- [Monoid.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Group.scala)
