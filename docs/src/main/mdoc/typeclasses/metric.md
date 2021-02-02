---
layout: docs
title:  "Metric"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Metric.scala"
scaladoc: "#com.twitter.algebird.Metric"
---

# Metric

A `Metric[V]` is a function `(V, V) => Double` that satisfies the following properties:

```
1. m(v1, v2) >= 0
2. m(v1, v2) == 0 iff v1 == v2
3. m(v1, v2) == m(v2, v1)
4. m(v1, v3) <= m(v1, v2) + m(v2, v3)
```

If you implement this trait, make sure that you follow these rules.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/metric.md). These links might be helpful:

- [Metric.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Metric.scala)
- [MetricProperties.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/MetricProperties.scala)
