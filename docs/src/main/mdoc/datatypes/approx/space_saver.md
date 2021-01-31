---
layout: docs
title:  "SpaceSaver"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/SpaceSaver.scala"
scaladoc: "#com.twitter.algebird.SpaceSaver"
---

# SpaceSaver

also called StreamSummary.

## Serialization
You can serialize `SpaceSaver` instances using `fromBytes` and `toBytes`:

```scala mdoc
import com.twitter.algebird._
import scala.util.Try

def stringToArrayByte(s: String): Array[Byte] = s.getBytes("UTF-8")

def arrayByteToString(a: Array[Byte]): Try[String] = Try(new String(a, "UTF-8"))

val ss = SpaceSaver[String](10, "word")
val bytes = SpaceSaver.toBytes(ss, stringToArrayByte)
val trySpaceSaver = SpaceSaver.fromBytes(bytes, arrayByteToString)

assert(ss == trySpaceSaver.get)
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/approx/space_hasher.md). These links might be helpful:

- [SpaceSaver.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/SpaceSaver.scala)
- [SpaceSaverTest.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/SpaceSaverTest.scala)

