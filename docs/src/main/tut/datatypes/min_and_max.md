---
layout: docs
title:  "Min and Max"
section: "data"
source: "algebird-core/src/main/scala/com/twitter/algebird/OrderedSemigroup.scala"
scaladoc: "#com.twitter.algebird.OrderedSemigroup"
---

# Min and Max

## Min

```tut:book
import com.twitter.algebird._
import Operators._
Min(10) + Min(20) + Min(30)
```

## Max

Example from <http://www.michael-noll.com/blog/2013/12/02/twitter-algebird-monoid-monad-for-large-scala-data-analytics/>

```tut:book
Max(10) + Max(30) + Max(20)

case class TwitterUser(val name: String, val numFollowers: Int) extends Ordered[TwitterUser] {
  def compare(that: TwitterUser): Int = {
    val c = this.numFollowers - that.numFollowers
    if (c == 0) this.name.compareTo(that.name) else c
  }
}
```

Let's have a popularity contest on Twitter.  The user with the most followers wins!

```tut
val barackobama = TwitterUser("BarackObama", 40267391)
val katyperry = TwitterUser("katyperry", 48013573)
val ladygaga = TwitterUser("ladygaga", 40756470)
val miguno = TwitterUser("miguno", 731) // I participate, too.  Olympic spirit!
val taylorswift = TwitterUser("taylorswift13", 37125055)
val winner: Max[TwitterUser] = Max(barackobama) + Max(katyperry) + Max(ladygaga) + Max(miguno) + Max(taylorswift)

assert(winner.get == katyperry)
```

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/min_and_max.md). These links might be helpful:

- [OrderedSemigroup.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/OrderedSemigroup.scala)
