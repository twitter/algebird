---
layout: docs
title:  "Roller"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/macros/Roller.scala"
scaladoc: "#com.twitter.algebird.macros.Roller"
---

# Roller

Given a `TupleN`, produces a sequence of `(N + 1)` tuples each of arity `N` such that, for all `k` from `0 to N`, there is a tuple with `k` `Some`s followed by `(N - k)` `None`s.

This is useful for comparing some metric across multiple layers of some hierarchy.

For example, suppose we have some climate data represented as

```scala
case class Data(continent: String, country: String, city: String, temperature: Double)
```

and we want to know the average temperatures of

- each continent
- each (continent, country) pair
- each (continent, country, city) triple

Here we desire the (continent, country) and (continent, country, city) pair because, for example, if we grouped by city instead of by (continent, country, city), we would accidentally combine the results for Paris, Texas and Paris, France.

Then we could do

```scala
import com.twitter.algebird.macros.Roller.roller
val data: List[Data]
val averageTemps: Map[(Option[String], Option[String], Option[String]), Double] =
data.flatMap { d => roller((d.continent, d.country, d.city)).map((_, d)) }
  .groupBy(_._1)
  .mapValues { xs => val temps = xs.map(_.temperature); temps.sum / temps.length }
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/combinator/roller.md). These links might be helpful:

- [Roller.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/macros/Roller.scala)
