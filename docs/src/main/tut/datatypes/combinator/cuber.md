---
layout: docs
title:  "Cuber"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/macros/Cuber.scala"
scaladoc: "#com.twitter.algebird.macros.Cuber"
---

# Cuber

"Cubes" a case class or tuple, i.e. for a tuple of type `(T1, T2, ... , TN)` generates all `2^N` possible combinations of type `(Option[T1], Option[T2], ... , Option[TN])`.

This is useful for comparing some metric across all possible subsets.

For example, suppose we have a set of people represented as


```scala
case class Person(gender: String, age: Int, height: Double)
```

and we want to know the average height of

- people, grouped by gender and age
- people, grouped by only gender
- people, grouped by only age
- all people

Then we could do

```scala
import com.twitter.algebird.macros.Cuber.cuber
val people: List[People]
val averageHeights: Map[(Option[String], Option[Int]), Double] =
  people.flatMap { p => cuber((p.gender, p.age)).map((_,p)) }
    .groupBy(_._1)
    .mapValues { xs => val heights = xs.map(_.height); heights.sum / heights.length }
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/combinator/cuber.md). These links might be helpful:

- [Cuber.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Cuber.scala)
