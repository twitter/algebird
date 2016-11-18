---
layout: docs
title:  "Buffered"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/BufferedOperation.scala"
scaladoc: "#com.twitter.algebird.Buffered"
---

# Buffered

Represents something that consumes `I` and may emit `O`. Has some internal state that may be used to improve performance. Generally used to model folds or reduces.

## BufferedReduce

Version of `Buffered` that never emits on `put`. Used in `sumOption` for the generated tuple semigroups and case class semigroups and `BufferedSumAll`.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/buffered.md). These links might be helpful:

- [BufferedOperation.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/BufferedOperation.scala)
