---
layout: docs
title:  "Ring"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Ring.scala"
scaladoc: "#com.twitter.algebird.Ring"
---

## Ring

Whereas a group is defined by a set and a single operation, a ring is defined by a set and two operations.  Given a set *R* and operations `*` and `+`, we say that `(R, +, *)` is a ring if it satisfies the following properties:

- `(R, +)` is an abelian group
- For any x and y &isin; R, `x * y` &isin; R.
- For any x, y, and z &isin; R, `(x * y) * z = x * (y * z)`.
- For any x, y, and z &isin; R, `x * (y + z) = x * y + x * z` and `(x + y) * z = x * z + y * z`

Notes on rings:

- A ring necessarily has an identity under `+` (called the *additive identity*), typically referred to as *zero*, **0**).
- A ring does not necessarily have an identity under `*` (called the *multiplicative identity*).
- A ring is not necessarily commutitive under `*`.
- A ring does not necessarily satisfy the inverse property under `*`.
- For any x &isin; R, `0*x = x*0 = 0`.

A few other types of rings:

- We say that `(R, +, *)` is a *ring with unity* if it has a multiplicative identity, referred to as *one*, or **1**.
- We say that `(R, +, *)` is a *commutative ring* if it satisfies the commutative property under multiplication, that is `x * y = y * x` &forall; x,y &isin; R.

## Examples of rings

- `(Z, +, *)`
- The set of square square matrices of a given size are a ring.

## AdjoinedUnitRing

Defined in [this file](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/AdjoinedUnitRing.scala). This is for the case where your `Ring[T]` is a `Rng` (i.e. there is no unit). [see this page](http://en.wikipedia.org/wiki/Pseudo-ring#Adjoining_an_identity_element).


### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/ring.md). These links might be helpful:

- [Ring.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Ring.scala)
