---
layout: docs
title:  "Predecessible"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Predecessible.scala"
scaladoc: "#com.twitter.algebird.Predecessible"
---

# Predecessible

This is a typeclass to represent things which are countable down. Note that it is important that a value `prev(t)` is always less than `t`. Note that `prev` returns `Option` because this class comes with the notion that some items may reach a minimum key, which is `None`.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/predecessible.md). These links might be helpful:

- [Predecessible.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Predecessible.scala)
- [PredecessibleTests.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/PredecessibleTests.scala)
