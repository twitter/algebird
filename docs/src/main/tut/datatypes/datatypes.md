# Algebird's Data Types

Algebird is, first and foremost, a library of data structures for large-scale analytics. Here are the data structures currently implemented:

*(NOTE: This list is not yet complete! We're working on an overhaul of the documentation site now. We'll remove this note when the list becomes comprehensive.)*

# Table of Contents

* Overview
* Preliminaries
* Max
* Hyperloglog
* Bloom Filter
* QTree
* Count Min Sketch
* Decayed Value (aka. moving average)
* Sketch Map
* Min
* General - Adding and Multiplication of collections

# Overview

The goal of this page is to show you how easy it is to learn and experiment with Algebird data structures with a REPL. Most of the examples are taken from Algebird tests which, incidentally, are a great way to learn how to use Algebird until the documentation is fleshed out.

It is helpful to have a conceptual understanding of Algebird. Here are two good overview documents on Scalding and Algebird monoids:
* http://www.michael-noll.com/blog/2013/12/02/twitter-algebird-monoid-monad-for-large-scala-data-analytics/
* https://speakerdeck.com/samklr/algebird-abstract-algebra-for-big-data-analytics

# Preliminaries

```
$ git clone https://github.com/twitter/algebird.git
$ cd algebird
$ ./sbt "project algebird-core" console
scala> import com.twitter.algebird._
```

# Max

Example from http://www.michael-noll.com/blog/2013/12/02/twitter-algebird-monoid-monad-for-large-scala-data-analytics/

```scala

scala> import com.twitter.algebird.Operators._
import com.twitter.algebird.Operators._

scala> Max(10) + Max(30) + Max(20)
res22: com.twitter.algebird.Max[Int] = Max(30)

scala> case class TwitterUser(val name: String, val numFollowers: Int) extends Ordered[TwitterUser] {
         def compare(that: TwitterUser): Int = {
           val c = this.numFollowers - that.numFollowers
           if (c == 0) this.name.compareTo(that.name) else c
           }
       }
res23: defined class TwitterUser

scala> // Let's have a popularity contest on Twitter.  The user with the most followers wins!

scala> val barackobama = TwitterUser("BarackObama", 40267391)
res24: barackobama: TwitterUser = TwitterUser(BarackObama,40267391)

scala> val katyperry = TwitterUser("katyperry", 48013573)
res25: katyperry: TwitterUser = TwitterUser(katyperry,48013573)

scala> val ladygaga = TwitterUser("ladygaga", 40756470)
res26: ladygaga: TwitterUser = TwitterUser(ladygaga,40756470)

scala> val miguno = TwitterUser("miguno", 731) // I participate, too.  Olympic spirit!
res27: miguno: TwitterUser = TwitterUser(miguno,731)

scala> val taylorswift = TwitterUser("taylorswift13", 37125055)
res28: taylorswift: TwitterUser = TwitterUser(taylorswift13,37125055)

scala> val winner: Max[TwitterUser] = Max(barackobama) + Max(katyperry) + Max(ladygaga) + Max(miguno) + Max(taylorswift)
res29: winner: com.twitter.algebird.Max[TwitterUser] = Max(TwitterUser(katyperry,48013573))
```


# Hyperloglog
```scala
import HyperLogLog._
val hll = new HyperLogLogMonoid(4)
val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
val seqHll = data.map { hll(_) }
val sumHll = hll.sum(seqHll)
val approxSizeOf = hll.sizeOf(sumHll)
val actualSize = data.toSet.size
val estimate = approxSizeOf.estimate

scala> val hll = new HyperLogLogMonoid(4)
hll: com.twitter.algebird.HyperLogLogMonoid = com.twitter.algebird.HyperLogLogMonoid@3ac80fd9

scala> val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
data: List[Int] = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)

scala> val seqHll = data.map { hll(_) }
seqHll: List[com.twitter.algebird.HLL] = List(SparseHLL(4,Map(2 -> Max(1))), SparseHLL(4,Map(2 -> Max(1))), SparseHLL(4,Map(2 -> Max(2))), SparseHLL(4,Map(2 -> Max(2))), SparseHLL(4,Map(13 -> Max(1))), SparseHLL(4,Map(13 -> Max(1))), SparseHLL(4,Map(4 -> Max(1))), SparseHLL(4,Map(4 -> Max(1))), SparseHLL(4,Map(12 -> Max(1))), SparseHLL(4,Map(12 -> Max(1))))

scala> val sumHll = hll.sum(seqHll)
sumHll: com.twitter.algebird.HLL = DenseHLL(4,Vector(0, 0, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0))

scala> val approxSizeOf = hll.sizeOf(sumHll)
approxSizeOf: com.twitter.algebird.Approximate[Long] = Approximate(1,4,9,0.9972)

scala> val actualSize = data.toSet.size
actualSize: Int = 5

scala> val estimate = approxSizeOf.estimate
estimate: Long = 4
```

# Bloom Filter
```scala
import com.twitter.algebird._
val NUM_HASHES = 6
val WIDTH = 32
val SEED = 1
val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
val bf = bfMonoid.create("1", "2", "3", "4", "100")
val approxBool = bf.contains("1")
val res = approxBool.isTrue

scala> val bfMonoid = new BloomFilterMonoid(NUM_HASHES, WIDTH, SEED)
bfMonoid: com.twitter.algebird.BloomFilterMonoid = BloomFilterMonoid(6,32,1)

scala> val bf = bfMonoid.create("1", "2", "3", "4", "100")
bf: com.twitter.algebird.BF =
BFSparse(<function1>, EWAHCompressedBitmap, size in bits = 32 size in words = 2
0 0x00
1 dirties
,32)

scala> val approxBool = bf.contains("1")
approxBool: com.twitter.algebird.ApproximateBoolean = ApproximateBoolean(true,0.9290349745708529)

scala> val res = approxBool.isTrue
res: Boolean = true
```

# QTree
```scala
import com.twitter.algebird.QTree
val data = List(1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8)
val seqQTree = data.map { QTree(_) }
import com.twitter.algebird.QTreeSemigroup
val qtSemigroup = new QTreeSemigroup[Long](6)
val sum = qtSemigroup.sumOption(seqQTree).get
val sum2 = seqQTree.reduce{qtSemigroup.plus(_,_)}
sum.count
sum.upperBound
sum.lowerBound
sum.quantileBounds(.5)
sum.quantileBounds(.5)
sum.quantileBounds(.25)
sum.quantileBounds(.75)

scala> import com.twitter.algebird.QTree
import com.twitter.algebird.QTree

scala> val data = List(1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8)
data: List[Int] = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8)

scala> val seqQTree = data.map { QTree(_) }
seqQTree: List[com.twitter.algebird.QTree[Long]] = List(QTree(1,0,1,1,None,None), QTree(1,0,1,1,None,None), QTree(2,0,1,2,None,None), QTree(2,0,1,2,None,None), QTree(3,0,1,3,None,None), QTree(3,0,1,3,None,None), QTree(4,0,1,4,None,None), QTree(4,0,1,4,None,None), QTree(5,0,1,5,None,None), QTree(5,0,1,5,None,None), QTree(6,0,1,6,None,None), QTree(6,0,1,6,None,None), QTree(7,0,1,7,None,None), QTree(7,0,1,7,None,None), QTree(8,0,1,8,None,None), QTree(8,0,1,8,None,None))

scala> import com.twitter.algebird.QTreeSemigroup
import com.twitter.algebird.QTreeSemigroup

scala> val qtSemigroup = new QTreeSemigroup[Long](6)
qtSemigroup: com.twitter.algebird.QTreeSemigroup[Long] = com.twitter.algebird.QTreeSemigroup@610c4ff8

scala> val sum = qtSemigroup.sumOption(seqQTree).get
sum: com.twitter.algebird.QTree[Long] = QTree(0,4,16,0,Some(QTree(0,3,14,0,Some(QTree(0,2,6,0,Some(QTree(0,1,2,0,None,Some(QTree(1,0,2,2,None,None)))),Some(QTree(1,1,4,0,Some(QTree(2,0,2,4,None,None)),Some(QTree(3,0,2,6,None,None)))))),Some(QTree(1,2,8,0,Some(QTree(2,1,4,0,Some(QTree(4,0,2,8,None,None)),Some(QTree(5,0,2,10,None,None)))),Some(QTree(3,1,4,0,Some(QTree(6,0,2,12,None,None)),Some(QTree(7,0,2,14,None,None)))))))),Some(QTree(1,3,2,0,Some(QTree(2,2,2,0,Some(QTree(4,1,2,0,Some(QTree(8,0,2,16,None,None)),None)),None)),None)))


scala> val sum2 = seqQTree.reduce{qtSemigroup.plus(_,_)}
sum2: com.twitter.algebird.QTree[Long] = QTree(0,4,16,0,Some(QTree(0,3,14,0,Some(QTree(0,2,6,0,Some(QTree(0,1,2,0,None,Some(QTree(1,0,2,2,None,None)))),Some(QTree(1,1,4,0,Some(QTree(2,0,2,4,None,None)),Some(QTree(3,0,2,6,None,None)))))),Some(QTree(1,2,8,0,Some(QTree(2,1,4,0,Some(QTree(4,0,2,8,None,None)),Some(QTree(5,0,2,10,None,None)))),Some(QTree(3,1,4,0,Some(QTree(6,0,2,12,None,None)),Some(QTree(7,0,2,14,None,None)))))))),Some(QTree(1,3,2,0,Some(QTree(2,2,2,0,Some(QTree(4,1,2,0,Some(QTree(8,0,2,16,None,None)),None)),None)),None)))

scala> sum.count
res120: Long = 16


scala> sum.upperBound
res122: Double = 16.0

scala> sum.lowerBound
res123: Double = 0.0

scala> sum.quantileBounds(.5)
res124: (Double, Double) = (5.0,6.0)

scala> sum.quantileBounds(.5)
res125: (Double, Double) = (5.0,6.0)

scala> sum.quantileBounds(.25)
res126: (Double, Double) = (3.0,4.0)

scala> sum.quantileBounds(.75)
res127: (Double, Double) = (7.0,8.0)
```

# Count Min Sketch
```scala
import com.twitter.algebird._
import com.twitter.algebird.CMSHasherImplicits._
val DELTA = 1E-10
val EPS = 0.001
val SEED = 1
val HEAVY_HITTERS_PCT = 0.01
val CMS_MONOID = TopPctCMS.monoid[Long](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
val data = List(1L, 1L, 3L, 4L, 5L)
val cms = CMS_MONOID.create(data)
cms.totalCount
cms.frequency(1L).estimate
cms.frequency(2L).estimate
cms.frequency(3L).estimate
val data = List("1", "2", "3", "4", "5")
val cms = CMS_MONOID.create(data)


scala> import com.twitter.algebird._
scala> import com.twitter.algebird.CMSHasherImplicits._
scala> val DELTA = 1E-10
scala> val EPS = 0.001
scala> val SEED = 1
scala> val HEAVY_HITTERS_PCT = 0.01

scala> val CMS_MONOID = TopPctCMS.monoid[Long](EPS, DELTA, SEED, HEAVY_HITTERS_PCT)
CMS_MONOID: com.twitter.algebird.CountMinSketchMonoid = com.twitter.algebird.CountMinSketchMonoid@2972781c

scala> val data = List(1L, 1L, 3L, 4L, 5L)
data: List[Long] = List(1, 1, 3, 4, 5)

scala> val cms = CMS_MONOID.create(data)
cms: com.twitter.algebird.CMS = CMSInstance(CMSCountsTable(Vector(Vector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0...

scala> cms.totalCount
res5: Long = 5

scala> cms.frequency(1L).estimate
res6: Long = 2

scala> cms.frequency(2L).estimate
res7: Long = 0

scala> cms.frequency(3L).estimate
res8: Long = 1

scala> val data = List("1", "2", "3", "4", "5")
data: List[java.lang.String] = List(1, 2, 3, 4, 5)

scala> val cms = CMS_MONOID.create(data)
<console>:18: error: overloaded method value create with alternatives:
  (data: Seq[Long])com.twitter.algebird.CMS <and>
  (item: Long)com.twitter.algebird.CMS
 cannot be applied to (List[String])
       val cms = CMS_MONOID.create(data)
                            ^

```
# Decayed Value (aka. moving average)
```scala
import com.twitter.algebird.Monoid
import com.twitter.algebird.DecayedValue

val data = {
  val rnd = new scala.util.Random
  (1 to 100).map { _ => rnd.nextInt(1000).toDouble }.toSeq
}

val HalfLife = 10.0
val normalization = HalfLife / math.log(2)

implicit val monoid = DecayedValue.monoidWithEpsilon(1e-3)

data.zipWithIndex.scanLeft(Monoid.zero[DecayedValue]) { (previous, data) =>
  val (value, time) = data
  val decayed = Monoid.plus(previous, DecayedValue.build(value, time, HalfLife))
  println("At %d: decayed=%f".format(time, (decayed.value / normalization)))
  decayed
}

At 0: decayed=7.278045
At 1: decayed=13.375555
At 2: decayed=56.910568
At 3: decayed=84.706949
At 4: decayed=122.564021
At 5: decayed=156.638253
At 6: decayed=209.640940
At 7: decayed=211.682928
At 8: decayed=207.488475
At 9: decayed=260.967498
At 10: decayed=266.573087
At 11: decayed=297.796305
At 12: decayed=284.507990
At 13: decayed=279.110341
At 14: decayed=304.919205
At 15: decayed=301.204525
At 16: decayed=324.355458
At 17: decayed=324.745738
At 18: decayed=317.970467
At 19: decayed=325.581173
At 20: decayed=357.288938
At 21: decayed=375.089827
At 22: decayed=366.329457
At 23: decayed=398.011706
At 24: decayed=378.566783
At 25: decayed=402.844636
At 26: decayed=409.069086
At 27: decayed=426.175002
At 28: decayed=454.473406
At 29: decayed=453.428122
At 30: decayed=435.817305
At 31: decayed=459.311110
At 32: decayed=495.787695
At 33: decayed=529.267035
At 34: decayed=512.330635
At 35: decayed=488.557222
At 36: decayed=474.624295
At 37: decayed=454.277054
At 38: decayed=465.374995
At 39: decayed=492.781161
At 40: decayed=519.599682
At 41: decayed=500.953975
At 42: decayed=478.912829
At 43: decayed=474.151469
At 44: decayed=507.693428
At 45: decayed=520.551467
At 46: decayed=552.025878
At 47: decayed=570.371501
At 48: decayed=564.961290
At 49: decayed=556.794221
At 50: decayed=529.142124
At 51: decayed=551.584848
At 52: decayed=559.216225
At 53: decayed=532.718913
At 54: decayed=522.482822
At 55: decayed=502.188431
At 56: decayed=484.362130
At 57: decayed=514.447723
At 58: decayed=530.596442
At 59: decayed=533.949542
At 60: decayed=513.926980
At 61: decayed=492.472680
At 62: decayed=513.489423
At 63: decayed=507.937495
At 64: decayed=517.937286
At 65: decayed=528.029884
At 66: decayed=527.673235
At 67: decayed=527.409784
At 68: decayed=494.586058
At 69: decayed=510.747874
At 70: decayed=507.805555
At 71: decayed=535.836009
At 72: decayed=523.866252
At 73: decayed=509.024394
At 74: decayed=484.016781
At 75: decayed=481.408954
At 76: decayed=516.197769
At 77: decayed=486.550893
At 78: decayed=514.549099
At 79: decayed=512.045370
At 80: decayed=539.098749
At 81: decayed=547.011765
At 82: decayed=577.823244
At 83: decayed=590.282412
At 84: decayed=608.284180
At 85: decayed=587.165274
At 86: decayed=609.673300
At 87: decayed=589.639719
At 88: decayed=595.831710
At 89: decayed=559.742952
At 90: decayed=530.229834
At 91: decayed=540.261698
At 92: decayed=544.423154
At 93: decayed=511.569129
At 94: decayed=488.539859
At 95: decayed=465.527867
At 96: decayed=463.880928
At 97: decayed=455.967326
At 98: decayed=473.051770
At 99: decayed=455.928999
res0: scala.collection.immutable.Seq[com.twitter.algebird.DecayedValue] = Vector(DecayedValue(0.0,-Infinity), DecayedValue(105.0,0.0), DecayedValue(192.96846411136477,0.06931471805599453), DecayedValue(821.0459433420897,0.13862943611198905), DecayedValue(1222.0629527056299,0.20794415416798356), DecayedValue(1768.2250526092378,0.2772588722239781), DecayedValue(2259.8123105463255,0.34657359027997264), DecayedValue(3024.479440420743,0.4158883083359671), DecayedValue(3053.939100137335,0.48520302639196167), DecayedValue(2993.4259345723635,0.5545177444479562), DecayedValue(3764.965154677916,0.6238324625039507), DecayedValue(3845.8367013009747,0.6931471805599453), DecayedValue(4296.292522376896,0.7624618986159398), DecayedValue(4104.582664670532,0.8317766166719343), DecayedValue(4026.711042627...
```

# Sketch Map
```scala
import com.twitter.algebird._
val DELTA = 1E-8
val EPS = 0.001
val SEED = 1
val HEAVY_HITTERS_COUNT = 10

implicit def string2Bytes(i : String) = i.toCharArray.map(_.toByte)


val PARAMS = SketchMapParams[String](SEED, EPS, DELTA, HEAVY_HITTERS_COUNT)
val MONOID = SketchMap.monoid[String, Long](PARAMS)
val data = List( ("1", 1L), ("3", 2L), ("4", 1L), ("5", 1L) )
val sm = MONOID.create(data)
sm.totalValue
MONOID.frequency(sm, "1")
MONOID.frequency(sm, "2")
MONOID.frequency(sm, "3")


scala> import com.twitter.algebird._
scala> val DELTA = 1E-8
scala> val EPS = 0.001
scala> val SEED = 1
scala> val HEAVY_HITTERS_COUNT = 10

scala> implicit def string2Bytes(i : String) = i.toCharArray.map(_.toByte)
string2Bytes: (i: String)Array[Byte]

scala> val PARAMS = SketchMapParams[String](SEED, EPS, DELTA, HEAVY_HITTERS_COUNT)
PARAMS: com.twitter.algebird.SketchMapParams[String] = SketchMapParams(1,0.0010,1.0E-8,10)

scala> val MONOID = SketchMap.monoid[String, Long](PARAMS)
MONOID: com.twitter.algebird.SketchMapMonoid[String,Long] = com.twitter.algebird.SketchMapMonoid@72ab3e61

scala> val data = List( ("1", 1L), ("3", 2L), ("4", 1L), ("5", 1L) )
data: List[(java.lang.String, Long)] = List((1,1), (3,2), (4,1), (5,1))

scala> val sm = MONOID.create(data)
sm: com.twitter.algebird.SketchMap[String,Long] = SketchMap(AdaptiveMatrix(DenseVector(SparseVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0...

scala> sm.totalValue
res18: Long = 5

scala> MONOID.frequency(sm, "1")
res19: Long = 1

scala> MONOID.frequency(sm, "2")
res20: Long = 0

scala> MONOID.frequency(sm, "3")
res21: Long = 2
```

# Min
```scala
scala>  Min(10) + Min(20) + Min(30)
<console>:11: error: type mismatch;
 found   : com.twitter.algebird.Min[Int]
 required: String
               Min(10) + Min(20) + Min(30)
                            ^

scala> import com.twitter.algebird.Operators._
import com.twitter.algebird.Operators._

scala>  Min(10) + Min(20) + Min(30)
res1: com.twitter.algebird.Min[Int] = Min(10)
```

# General - Adding and Multiplication of collections

```scala

scala> val data2 = Map(1 -> 1, 2 -> 1)
data2: scala.collection.immutable.Map[Int,Int] = Map(1 -> 1, 2 -> 1)

scala> val data1 =  Map(1 -> 3, 2 -> 5, 3 -> 7, 5 -> 1)
data1: scala.collection.immutable.Map[Int,Int] = Map(1 -> 3, 2 -> 5, 3 -> 7, 5 -> 1)

scala> data1 + data2
<console>:13: error: type mismatch;
 found   : scala.collection.immutable.Map[Int,Int]
 required: (Int, ?)
              data1 + data2
                      ^

scala>  import com.twitter.algebird.Operators._
import com.twitter.algebird.Operators._

scala> data1 + data2
res1: scala.collection.immutable.Map[Int,Int] = Map(1 -> 4, 2 -> 6, 3 -> 7, 5 -> 1)

scala> data1 * data2
res2: scala.collection.immutable.Map[Int,Int] = Map(1 -> 3, 2 -> 5)

scala> Set(1,2,3) + Set(3,4,5)
res3: scala.collection.immutable.Set[Int] = Set(5, 1, 2, 3, 4)

scala> List(1,2,3) + List(3,4,5)
res4: List[Int] = List(1, 2, 3, 3, 4, 5)

scala> Map(1 -> 3, 2 -> 4, 3 -> 1) * Map(2 -> 2)
res5: scala.collection.immutable.Map[Int,Int] = Map(2 -> 8)

scala> Map(1 -> Set(2,3), 2 -> Set(1)) + Map(2 -> Set(2,3))
res6: scala.collection.immutable.Map[Int,scala.collection.immutable.Set[Int]] = Map(1 -> Set(2, 3), 2 -> Set(1, 2, 3))
```

## Approximate Data Structures

#### [Count Min Sketch](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/CountMinSketch.scala)

Count-min sketch is a probablistic data structure that estimates the frequencies of elements in a data stream. Count-min sketches are somewhat similar to Bloom filters; the main distinction is that Bloom filters represent sets, while count-min sketches represent multisets. For more info, see [Wikipedia](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch).

In Algebird, count-min sketches are represented as the abstract class CMS, along with the CMSMonoid class.

#### [HyperLogLog](https://github.com/twitter/algebird/blob/develop/algebird-core/src/main/scala/com/twitter/algebird/HyperLogLog.scala)]



... and many more.
