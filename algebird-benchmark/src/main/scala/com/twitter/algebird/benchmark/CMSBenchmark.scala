// package com.twitter.algebird.benchmark

// import com.google.caliper.{ Param, SimpleBenchmark }
// import com.twitter.algebird.{ TopPctCMS, CMSHasherImplicits, TopPctCMSMonoid }

// /**
//  * Benchmarks the Count-Min sketch implementation in Algebird.
//  *
//  * We benchmark different `K` types as well as different input data streams.
//  */
// // Once we can convince cappi (https://github.com/softprops/capp) -- the sbt plugin we use to run
// // caliper benchmarks -- to work with the latest caliper 1.0-beta-1, we would:
// //     - Let `CMSBenchmark` extend `Benchmark` (instead of `SimpleBenchmark`)
// //     - Annotate `timePlus` with `@MacroBenchmark`.
// class CMSBenchmark {

//   val Seed = 1
//   val JavaCharSizeInBits = 2 * 8

//   @Param(Array("0.1", "0.005"))
//   val eps: Double = 0.0

//   @Param(Array("0.0000001" /* 1E-8 */ ))
//   val delta: Double = 0.0

//   @Param(Array("0.2"))
//   val heavyHittersPct: Double = 0.0

//   @Param(Array("100"))
//   val operations: Int = 0 // Number of operations per benchmark repetition (cf. `reps`)

//   @Param(Array("2048"))
//   val maxBits: Int = 0

//   var random: scala.util.Random = _
//   var cmsLongMonoid: TopPctCMSMonoid[Long] = _
//   var cmsBigIntMonoid: TopPctCMSMonoid[BigInt] = _
//   var cmsStringMonoid: TopPctCMSMonoid[String] = _
//   var inputsBigInt: Seq[BigInt] = _
//   var inputsString: Seq[String] = _

//   override def setUp() {
//     // Required import of implicit values (e.g. for BigInt- or Long-backed CMS instances)
//     import CMSHasherImplicits._

//     cmsLongMonoid = TopPctCMS.monoid[Long](eps, delta, Seed, heavyHittersPct)
//     cmsBigIntMonoid = TopPctCMS.monoid[BigInt](eps, delta, Seed, heavyHittersPct)
//     cmsStringMonoid = TopPctCMS.monoid[String](eps, delta, Seed, heavyHittersPct)

//     random = new scala.util.Random

//     inputsString = (0 to operations).map { i => random.nextString(maxBits / JavaCharSizeInBits) }.toSeq
//     Console.out.println(s"Created ${inputsString.size} input records for String")
//     inputsBigInt = inputsString.map { s => BigInt(s.getBytes) }
//     Console.out.println(s"Created ${inputsBigInt.size} input records for BigInt")
//   }

//   // Case A (K=Long): We count the first hundred integers, i.e. [1, 100]
//   def timePlusOfFirstHundredIntegersWithLongCms(reps: Int): Int = {
//     var dummy = 0
//     while (dummy < reps) {
//       (1 to operations).view.foldLeft(cmsLongMonoid.zero)((l, r) => { l ++ cmsLongMonoid.create(r) })
//       dummy += 1
//     }
//     dummy
//   }

//   // Case B.1 (K=BigInt): We count the first hundred integers, i.e. [1, 100]
//   def timePlusOfFirstHundredIntegersWithBigIntCms(reps: Int): Int = {
//     var dummy = 0
//     while (dummy < reps) {
//       (1 to operations).view.foldLeft(cmsBigIntMonoid.zero)((l, r) => { l ++ cmsBigIntMonoid.create(r) })
//       dummy += 1
//     }
//     dummy
//   }

//   // Case B.2 (K=BigInt): We count numbers drawn randomly from a 2^maxBits address space
//   def timePlusOfRandom2048BitNumbersWithBigIntCms(reps: Int): Int = {
//     var dummy = 0
//     while (dummy < reps) {
//       inputsBigInt.view.foldLeft(cmsBigIntMonoid.zero)((l, r) => l ++ cmsBigIntMonoid.create(r))
//       dummy += 1
//     }
//     dummy
//   }

//   // Case C (K=String): We count strings drawn randomly from a 2^maxBits address space
//   def timePlusOfRandom2048BitNumbersWithStringCms(reps: Int): Int = {
//     var dummy = 0
//     while (dummy < reps) {
//       inputsString.view.foldLeft(cmsStringMonoid.zero)((l, r) => l ++ cmsStringMonoid.create(r))
//       dummy += 1
//     }
//     dummy
//   }

// }