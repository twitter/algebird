---
layout: docs
title:  "HyperLogLog Series"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/HyperLogLogSeries.scala"
scaladoc: "#com.twitter.algebird.HyperLogLogSeries"
---

# HyperLogLog Series

`HyperLogLogSeries` can produce a `HyperLogLog` counter for any window into the past, using a constant factor more space than HyperLogLog.

For each hash bucket, rather than keeping a single max RhoW value, it keeps every RhoW value it has seen, and the max timestamp where it saw that value.  This allows it to reconstruct an HLL as it would be had it started at zero at any given point in the past, and seen the same updates this structure has seen.

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/approx/hyperloglog_series.md). These links might be helpful:

- [HyperLogLogSeries.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLogSeries.scala)
- [HyperLogLogSeriesTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/HyperLogLogSeriesTest.scala)
