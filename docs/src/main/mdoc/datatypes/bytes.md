---
layout: docs
title:  "Bytes"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/Bytes.scala"
scaladoc: "#com.twitter.algebird.Bytes"
---

# Bytes

A wrapper for `Array[Byte]` that provides sane implementations of `hashCode`, `equals`, and `toString`.  The wrapped array of bytes is assumed to be never modified.

Note: Unfortunately we cannot make `Bytes` a value class because a value class may not override the `hashCode` and `equals` methods (cf. SIP-15, criterion 4).

=Alternatives=

Instead of wrapping an `Array[Byte]` with this class you can also convert an `Array[Byte]` to a `Seq[Byte]` via Scala's `toSeq` method:

```scala
val arrayByte: Array[Byte] = Array(1.toByte)
val seqByte: Seq[Byte] = arrayByte.toSeq
```

Like `Bytes`, a `Seq[Byte]` has sane `hashCode`, `equals`, and `toString` implementations.

Performance-wise we found that a `Seq[Byte]` is comparable to [[Bytes]]. For example, a `CMS[Seq[Byte]]` was measured to be only slightly slower than `CMS[Bytes]` (think: single-digit percentages).

## Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/bytes.md). These links might be helpful:

- [Bytes.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Bytes.scala)
- [BytesSpec.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/BytesSpec.scala)
