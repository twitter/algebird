---
layout: docs
title:  "SummingIterator"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/SummingIterator.scala"
scaladoc: "#com.twitter.algebird.SummingIterator"
---

# SummingIterator

Creates an `Iterator` that emits partial sums of an input `Iterator[V]`. Generally this is useful to change from processing individual `V`s to possibly blocks of `V`.

@see `SummingQueue` or a cache of recent Keys in a V=Map[K,W] case: @see `SummingCache`.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/summer/summingiterator.md). These links might be helpful:

- [SummingIterator.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/SummingIterator.scala)
- [SummingIteratorTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/SummingIteratorTest.scala)
