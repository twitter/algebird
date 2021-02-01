---
layout: docs
title:  "Monad"
section: "typeclasses"
source: "algebird-core/src/main/scala/com/twitter/algebird/Monad.scala"
scaladoc: "#com.twitter.algebird.Monad"
---

# Monad

*(Note - don't use this, use [cats](https://github.com/typelevel/cats))*

Simple implementation of a `Monad` type class. Subclasses only need to override `apply` and `flatMap`, but they should override `map`, `join`, `joinWith`, and `sequence` if there are better implementations.

Laws `Monad` instances must follow:

identities:

```scala
flatMap(apply(x))(fn) == fn(x)
flatMap(m)(apply _) == m
```

associativity on `flatMap` (you can either `flatMap` `f` first, or `f` to `g`):

```scala
flatMap(flatMap(m)(f))(g) == flatMap(m) { x => flatMap(f(x))(g) }
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/typeclasses/monad.md). These links might be helpful:

- [Monad.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Monad.scala)
- [MonadProperties.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/MonadProperties.scala)
