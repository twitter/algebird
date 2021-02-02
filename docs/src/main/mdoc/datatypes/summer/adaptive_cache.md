---
layout: docs
title:  "AdaptiveCache"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/AdaptiveCache.scala"
scaladoc: "#com.twitter.algebird.AdaptiveCache"
---

# AdaptiveCache

This is a wrapper around `SummingCache` that attempts to grow the capacity by up to some maximum, as long as there's enough RAM.  It determines that there's enough RAM to grow by maintaining a `SentinelCache` which keeps caching and summing the evicted values. Once the `SentinelCache` has grown to the same size as the current cache, plus some margin, without running out of RAM, then this indicates that we have enough headroom to double the capacity.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/summer/adaptive_cache.md). These links might be helpful:

- [AdaptiveCache.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/AdaptiveCache.scala)
