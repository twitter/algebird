---
layout: docs
title:  "Scala Collections"
section: "data"
---

# Scala Collections

## Adding and Multiplication

```scala mdoc
val data2 = Map(1 -> 1, 2 -> 1)
val data1 =  Map(1 -> 3, 2 -> 5, 3 -> 7, 5 -> 1)
import com.twitter.algebird.Operators._
data1 + data2
data1 * data2
Set(1,2,3) + Set(3,4,5)
List(1,2,3) + List(3,4,5)
Map(1 -> 3, 2 -> 4, 3 -> 1) * Map(2 -> 2)
Map(1 -> Set(2,3), 2 -> Set(1)) + Map(2 -> Set(2,3))
```

## Other Collections

- [IndexedSeq.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/IndexedSeq.scala)
- [MapAlgebra.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/MapAlgebra.scala)
- PriorityQueue aggregator, monoid

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/combinator/collections.md).
