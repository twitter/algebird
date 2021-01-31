---
layout: docs
title:  "Functor"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Functor.scala"
scaladoc: "#com.twitter.algebird.Functor"
---

# Functor

*(Note - don't use this, use [cats](https://github.com/typelevel/cats))*

Simple implementation of a `Functor` type-class.

Laws Functors must follow:

```scala
map(m)(id) == m
map(m)(f andThen g) == map(map(m)(f))(g)
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/functor.md). These links might be helpful:

- [Functor.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Functor.scala)
- [FunctorProperties.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/FunctorProperties.scala)
