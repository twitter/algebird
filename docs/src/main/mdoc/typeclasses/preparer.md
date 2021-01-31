---
layout: docs
title:  "Preparer"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Preparer.scala"
scaladoc: "#com.twitter.algebird.Preparer"
---

# Preparer

`Preparer` is a way to build up an `Aggregator` through composition using a more natural API. It allows you to start with the input type and describe a series of transformations and aggregations from there, rather than starting from the aggregation and composing "outwards" in both directions.

Uses of `Preparer` will always start with a call to `Preparer[A]`, and end with a call to `monoidAggregate` or a related method, to produce an `Aggregator` instance.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/preparer.md). These links might be helpful:

- [Preparer.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Preparer.scala)
- [PreparerLaws.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/PreparerLaws.scala)
