---
layout: docs
title:  "Min and Max"
section: "data"
---

# Min and Max

`Min[T]` and `Max[T]` are data structures that keep track of, respectively, the minimum and maximum instances of `T` that you've seen. `First[T]` works for any type `T` with an `Ordering[T]` instance:

```tut:book
import com.twitter.algebird._
Min(3) + Min(2) + Min(1)
Min("a") + Min("aaa") + Min("ccccc") // by length
```

As does `Max[T]`:

```tut:book
Max(3) + Max(2) + Max(1)
Max("a") + Max("aaa") + Max("ccccc") // by length
```

## Algebraic Properties

`Min[T]` and `Max[T]` are both commutative semigroups. For `Min[T]`, the `+` function keeps the input with the minimum wrapped instance of `T`, while `Max[T]`'s `+` implementation keeps the maximum input. For example, for `Min[T]`:

```tut:book
val min1 = Min(1) + Min(100) == Min(1)
val min2 = Min(100) + Min(1) == Min(1)
assert(min1 && min2)
```

And for `Max[T]`:

```tut:book
val max1 = Max(1) + Max(100) == Max(100)
val max2 = Max(100) + Max(1) == Max(100)
assert(max1 && max2)
```

`Min[T]` forms a monoid on numeric types with an upper bound, like `Int` and `Float`:

```tut:book
Monoid.zero[Min[Int]]
Monoid.zero[Min[Float]]
```

Since all instances of `T` will be less than or equal to the upper bound.

`Max[T]` forms a monoid on types with a *lower* bound. This includes the numeric types as well as collections like `List[T]` and `String`. The monoid instance for these containers compares each `T` element-wise, with the additional notion that "shorter" sequences are smaller. This allows us to use the empty collection as a lower bound.

```tut:book
Monoid.zero[Max[Int]]
Monoid.zero[Max[Float]]
Monoid.zero[String]
```

## Usage Examples

Let's have a popularity contest on Twitter. The user with the most followers wins! (We've borrowed this example with thanks from [Michael Noll](https://twitter.com/miguno)'s excellent algebird tutorial, [Of Algebirds, Monoids, Monads, and Other Bestiary for Large-Scale Data Analytics](http://www.michael-noll.com/blog/2013/12/02/twitter-algebird-monoid-monad-for-large-scala-data-analytics)). First, let's write a data structure to represent a pair of username and the user's number of followers:

```tut:book
case class TwitterUser(val name: String, val numFollowers: Int) extends Ordered[TwitterUser] {
  def compare(that: TwitterUser): Int = {
    val c = this.numFollowers - that.numFollowers
    if (c == 0) this.name.compareTo(that.name) else c
  }
}
```

Now let's create a bunch of `TwitterUser` instances.

```tut:book
val barackobama = TwitterUser("BarackObama", 40267391)
val katyperry = TwitterUser("katyperry", 48013573)
val ladygaga = TwitterUser("ladygaga", 40756470)
val miguno = TwitterUser("miguno", 731) // I participate, too.  Olympic spirit!
val taylorswift = TwitterUser("taylorswift13", 37125055)
```

Who's the winner? Since `TwitterUser` defines an `Ordering` by extending `Ordered`, we can find the winner by wrapping each user in `Max` and combining all of the `Max[TwitterUser]` instances with `+`:


```tut:book
val winner: Max[TwitterUser] = Max(barackobama) + Max(katyperry) + Max(ladygaga) + Max(miguno) + Max(taylorswift)
assert(katyperry == winner.get)
```

A similar trick with `Min[TwitterUser]` gives us the loser:

```tut:book
val loser: Min[TwitterUser] = Min(barackobama) + Min(katyperry) + Min(ladygaga) + Min(miguno) + Min(taylorswift)
assert(miguno == loser.get)
```

So sad.

## Related Data Structures

`Min[T]` and `Max[T]` are related to `First[T]` and `Last[T]`. All four of these data structures wrap up instances of another type `T` to make them combine in a different way. `Min[T]` and `Max[T]` force their combination to depend on the ordering of the type `T`. `First[T]` and `Last[T]` force them to combine based on the order that they're seen in a stream.

See [the docs on `First` and `Last`](first_and_last.html) for more information.

### Links

- Source: [Min.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Min.scala) and [Max.scala](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/Max.scala)
- Tests: [MinSpec.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/MinSpec.scala) and [MaxSpec.scala](https://github.com/twitter/algebird/blob/develop/algebird-test/src/test/scala/com/twitter/algebird/MaxSpec.scala)
- Scaladoc: [Min]({{site.baseurl}}/api#com.twitter.algebird.Min) and [Max]({{site.baseurl}}/api#com.twitter.algebird.Max)

### Documentation Help

We'd love your help fleshing out this documentation! You can edit this page in your browser by clicking [this link](https://github.com/twitter/algebird/edit/develop/docs/src/main/tut/datatypes/min_and_max.md).
