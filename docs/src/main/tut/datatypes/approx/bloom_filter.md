---
layout: docs
title:  "Bloom Filter"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/BloomFilter.scala"
scaladoc: "#com.twitter.algebird.BloomFilter"
---

# Bloom Filter

A Bloom filter is a data structure invented by Burton Howard Bloom in 1970. It makes it possible to check for set
membership in a space efficient way. It does in a probabilistic manner, probiding the property that there may be false
 positives, but its guaranteed to have no false negatives.

A Bloom filter is created by initializing a bit array of n bits, with all bits set to 0. Elements are then added by
computing m hash functions which output values correspond to the indexes in the byte array with a uniform random
distribution. As these hash functions are computed, the bytes in the array corresponding to the hash function output
are set to 1. This is done for all elements added to the set.

To query the filter for membership of a element, the same m hash functions are computed for that element, and the
corresponding bits are checked. If all bits are not set to 1, we know that the element was not part of the set. However,
if all bits are set to one the element might be a member of the set.

The size of a Bloom filter depends on the number of input elements, and the desired false positive rate.

To read more about Bloom filters see wikipedia: https://en.wikipedia.org/wiki/Bloom_filter

example usage:

```tut:book
import com.twitter.algebird._

// We need to provide a implicit conversion from of the Bloom filter,
// in this case String, to Hash128. Conveniently Hash128 has a number
// of built in conversions so we do not need to define our own.
implicit def stringToHash128 = Hash128.stringHash

// It's possible to create a Bloom filter with a set number of
// hashes, and with a set width.
val NUM_HASHES = 6
val WIDTH = 32
val bfMonoid1 = new BloomFilterMonoid(NUM_HASHES, WIDTH)
val bf1 = bfMonoid1.create("1", "2", "3", "4", "100")
val approxBool1 = bf1.contains("1")
val res1 = approxBool1.isTrue

// Out you can specify a estimate of the number of elements
// which will be added to the set, and a desired false positive
// frequency like this:
val bloomFilterMonoid2 = BloomFilter(numEntries = 100, fpProb = 0.01)
val bf2 = bloomFilterMonoid2.create("1", "2", "3", "4", "100")
val approxBool2 = bf2.contains("1")
val res2 = approxBool2.isTrue

```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/approx/bloom_filter.md). These links might be helpful:

- [BloomFilter.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/BloomFilter.scala)
- [BloomFilterTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/BloomFilterTest.scala)
