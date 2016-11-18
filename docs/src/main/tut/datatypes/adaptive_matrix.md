---
layout: docs
title:  "Adaptive Matrix"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/matrix/AdaptiveMatrix.scala"
scaladoc: "#com.twitter.algebird.matrix.AdaptiveMatrix"
---

# AdaptiveMatrix

A Matrix structure that is designed to hide moving between sparse and dense representations. Initial support here is focused on a dense row count with a sparse set of columns.

## Hashing Trick

TODO: Talk about the `HashingTrickMonoid`. The [PR](https://github.com/twitter/algebird/pull/154) has great info and links on what's going on here.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/adaptive_matrix.md). These links might be helpful:

- [AdaptiveMatrix.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/matrix/AdaptiveMatrix.scala)
- [HashingTrick.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HashingTrick.scala)
