---
layout: docs
title:  "SummingQueue"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/SummingQueue.scala"
scaladoc: "#com.twitter.algebird.SummingQueue"
---

# SummingQueue

used as intermediate buffer in SB realtime.

A useful utility for aggregation systems: you buffer up some number of items in a thread-safe way, and when you have at most K of them, you sum them all together. A good use-case of this is doing a limited preaggregation before sending on to a next phase (from mappers to reducers on Hadoop, or between storm bolts).

Without this finite buffer history, an aggregated item could build up infinite history, and therefore it is unbounded in the error you could introduce by losing the buffer.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/summer/summingqueue.md). These links might be helpful:

- [SummingQueue.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/SummingQueue.scala)
- [SummingQueueTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/SummingQueueTest.scala)
