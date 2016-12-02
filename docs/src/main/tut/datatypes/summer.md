---
layout: docs
title:  "Summers"
section: "data"
---

# Summers

TODO: Discuss what summers are, and improve the documentation below on `StatefulSummer`.

## StatefulSummer

A Stateful summer is something that is potentially more efficient (a buffer, a cache, etc...) that has the same result as a sum:

```
Law 1: Semigroup.sumOption(items) ==
  (Monoid.plus(items.map { stateful.put(_) }.filter { _.isDefined }, stateful.flush) &&
    stateful.isFlushed)
Law 2: isFlushed == flush.isEmpty
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/summer.md). These links might be helpful:

- [StatefulSummer.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/StatefulSummer.scala)
