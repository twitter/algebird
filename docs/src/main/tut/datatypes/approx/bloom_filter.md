---
layout: docs
title:  "Bloom Filter"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/BloomFilter.scala"
scaladoc: "#com.twitter.algebird.BloomFilter"
---

# Bloom Filter

example usage:

```tut:book
import com.twitter.algebird._
val NUM_HASHES = 6
val WIDTH = 32
val SEED = 1
val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
val bf = bfMonoid.create("1", "2", "3", "4", "100")
val approxBool = bf.contains("1")
val res = approxBool.isTrue
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/approx/bloom_filter.md). These links might be helpful:

- [BloomFilter.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/BloomFilter.scala)
- [BloomFilterTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/BloomFilterTest.scala)
