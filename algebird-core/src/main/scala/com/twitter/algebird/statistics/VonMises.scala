package com.twitter.algebird.statistics

import com.twitter.algebird.Monoid
import scala.util.Random
import scala.math

/**
 * Represents a Von Mises distribution, which is a distribution over
 * angles. Taken from
 *
 * https://github.com/rwl/ParallelColt/blob/master/src/main/java/cern/jet/random/tdouble/VonMises.java
 *
 * @param mu is the mean of the distribution, ~ gaussian mean
 * @param k is the concentration, which is like 1/gaussian variance
 */
case class VonMises(mu: Double, k: Double) {
  require(k >= 0, "K must be positive")
  require(mu <= math.Pi * 2 && mu >= 0, "Mu must be in the range [0,2pi]")

  private val r: Double = {
    val tau = 1.0 + math.sqrt(1.0 + 4.0 * k * k)
    val rho = (tau - math.sqrt(2.0 * tau)) / (2.0 * k)
    (1.0 + rho * rho) / (2 * rho)
  }

  // Taken from the colt implementation
  // https://github.com/rwl/ParallelColt/blob/master/src/main/java/cern/jet/random/tdouble/VonMises.java
  def nextDouble: Double = {
    val v = Random.nextDouble
    val u = Random.nextDouble
    val z = math.cos(math.Pi * u)
    val w = (1.0 + r * z) / (r + z)
    val c = k * (r - w)

    if (v <= (c * (2.0 - c)) || c <= math.log(c / v) + 1.0) {
      val choice = Random.nextDouble
      if (choice > 0.5) mu + math.acos(w) else mu - math.acos(w)
    } else draw
  }

  def mean = mu
  def mode = mean
  // def variance = 1 - Bessel.i1(k) / Bessel.i0(k)
  // def entropy = -k * Bessel.i1(k) / Bessel.i0(k) + math.log(2 * math.Pi * Bessel.i0(k))
}

// object VonMises {
//   type Parameter = (Double,Double)

//   case class SufficientStatistic(n: Double, sines: Double, cosines: Double) extends breeze.stats.distributions.SufficientStatistic[SufficientStatistic] {
//     def +(t: SufficientStatistic) = new SufficientStatistic(n + t.n, sines + t.sines, cosines + t.cosines)

//     def *(weight: Double) = SufficientStatistic(weight * n, weight * sines, weight * cosines)
//   }

//   def emptySufficientStatistic = SufficientStatistic(0,0,0)

//   def sufficientStatisticFor(t: Double) = SufficientStatistic(1,sin(t),cos(t))
//   def distribution(p: Parameter) = new VonMises(p._1,p._2)

//   def mle(stats: SufficientStatistic): (Double, Double) = {
//     import breeze.linalg.DenseVector.TupleIsomorphisms._
//     val lensed = likelihoodFunction(stats).throughLens[DenseVector[Double]]
//     // Starting points due to Sra, 2009... not as good as these old ones that I forgot about
//     // http://en.wikipedia.org/wiki/Von_Mises-Fisher_distribution
// //    val startingMu = {
// //      val m = asin(stats.sines/stats.n)
// //      if(m < 0)
// //        m + 2 * math.Pi
// //      else m
// //    }
// //    val rhat = sqrt(stats.sines * stats.sines + stats.cosines * stats.cosines) / stats.n
// //    val startingK = rhat * (2 - rhat * rhat) / (1-rhat * rhat)
//         val cosineSum = stats.cosines
//     val sineSum = stats.sines
//     val muPart = signum(cosineSum) * signum(sineSum) * atan(abs(sineSum/cosineSum))
//     val mu = (muPart + {
//       if(cosineSum < 0) Pi
//       else if (cosineSum > 0 && sineSum < 0) 2 * Pi
//       else 0.0
//     } ) % (2 * Pi)

//     val t = sqrt(pow(cosineSum/stats.n,2) + pow(sineSum / stats.n,2))
//     val k = (1.28 - 0.53*pow(t,2)) * tan(Pi/2*t)

//     val kx = {
//       if(t < 0.53) t * (2 + t *t * (1 + 5 * t * t / 6))
//       else if(t < 0.85) -0.4 + 1.39 * t + (0.43)/(1-t)
//       else 1/( t* (3 + t * (-4 + t)))
//     }
//     val result = minimize(lensed,DenseVector(mu,kx))
//     val res@(a,b) = (result(0),result(1))
//     res
//   }

//   def likelihoodFunction(stats: SufficientStatistic) = new DiffFunction[(Double,Double)] {
//     def calculate(x: (Double,Double)) = {
//       val DELTA = 1E-5
//       val (mu,k) = x
//       if( mu < 0 || mu > 2*Pi || k < 0) (Double.PositiveInfinity,(0.0,0.0))
//       else {
//         val (sinx,cosx) = (sin(mu),cos(mu))
//         val bessel_k = Bessel.i0(k)
//         val logprob = stats.n * math.log(bessel_k * 2* Pi) - (stats.sines * sinx + stats.cosines * cosx)*k
//         val mugrad = -k * (stats.sines * cos(mu) - stats.cosines * sin(mu))
//         val kgrad = stats.n * (Bessel.i1(k)/bessel_k)  - (stats.sines * sinx + stats.cosines * cosx)

//         (logprob,(mugrad,kgrad))

//       }

//     }
//   }

//   /*
//   /**
//    * Returns the maximum likelihood estimate of this distribution
//    * For the given observations with (possibly pseudo-)counts
//    */

//   def mle(obs: Counter[Double,Double]) = {
//     val sufStats = for {
//       (o,count) <- obs.pairs
//     } yield {
//       (count * cos(o),count * sin(o))
//     }
//     val cosineSum = sufStats.iterator.map(_._1) reduceLeft(_ + _)
//     val sineSum = sufStats.iterator.map(_._2) reduceLeft( _ + _ )
//     val muPart = signum(cosineSum) * signum(sineSum) * atan(abs(sineSum/cosineSum))
//     val mu = (muPart + {
//       if(cosineSum < 0) Pi
//       else if (cosineSum > 0 && sineSum < 0) 2 * Pi
//       else 0.0
//     } ) % (2 * Pi)

//     val t = sqrt(pow(cosineSum/obs.sum,2) + pow(sineSum / obs.sum,2))
//     val k = (1.28 - 0.53*pow(t,2)) * tan(Pi/2*t)

//     /*
//     val kx = {
//       if(t < 0.53) t * (2 + t *t * (1 + 5 * t * t / 6))
//       else if(t < 0.85) -0.4 + 1.39 * t + (0.43)/(1-t)
//       else 1/( t* (3 + t * (-4 + t)))
//     } */
//     VonMises(mu,k)
//   }
//   */
// }
