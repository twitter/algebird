---
layout: docs
title:  "Applicative"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Applicative.scala"
scaladoc: "#com.twitter.algebird.Applicative"
---

# Applicative

*(Note - don't use this, use [cats](https://github.com/typelevel/cats))*

Simple implementation of an `Applicative` type-class. There are many choices for the canonical second operation (`join`, `sequence`, `joinWith`, `ap`),
all equivalent. For a `Functor` modeling concurrent computations with failure, like `Future`, combining results with `join` can save a lot of time over combining with `flatMap`. (Given two operations, if the second fails before the first completes, one can fail the entire computation right then. With `flatMap`, one would have to wait for the first operation to complete before failing it.)

Laws `Applicative` instances must follow:

```scala
map(apply(x))(f) == apply(f(x))
join(apply(x), apply(y)) == apply((x, y))
```

(`sequence` and `joinWith` specialize `join` - they should behave appropriately.)

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/applicative.md). These links might be helpful:

- [Applicative.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Applicative.scala)
- [ApplicativeProperties.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/ApplicativeProperties.scala)
