---
layout: docs
title:  "Successible"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Successible.scala"
scaladoc: "#com.twitter.algebird.Successible"
---

# Successible

This is a typeclass to represent things which increase. Note that it is important that a value after being incremented is always larger than it was before. Note that next returns Option because this class comes with the notion of the "greatest" key, which is `None`. `Int`s, for example, will cycle if `next(java.lang.Integer.MAX_VALUE)` is called, therefore we need a notion of what happens when we hit the bounds at which our ordering is violating. This is also useful for closed sets which have a fixed progression.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/successible.md). These links might be helpful:

- [Successible.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Successible.scala)
- [SuccessibleProperties.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/SuccessibleProperties.scala)
