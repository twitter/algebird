package com.twitter.algebird

import org.specs._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.choose

object CountMinSketchLaws extends Properties("CountMinSketch") with BaseProperties {
  val DEPTH = 10
  val WIDTH = 30
  val SEED = 1
  
  implicit val cmsMonoid = new CountMinSketchMonoid(DEPTH, WIDTH, SEED)
  implicit val cmsGen = 
    Arbitrary { 
      for (v <- choose(0, 10000)) yield (cmsMonoid.create(v))
    }

  property("CountMinSketch is a Monoid") = monoidLaws[CMS]
}

class CountMinSketchTest extends Specification {
  noDetailedDiffs()

  val DEPTH = 10
  val WIDTH = 300
  val SEED = 1
  val MONOID = new CountMinSketchMonoid(DEPTH, WIDTH, SEED)
  val RAND = new scala.util.Random

  /**
   * Creates a Count-Min sketch out of the given data stream.
   */
  def createCountMinSketch(data : Seq[Long]) : CMS = {
    data.foldLeft(MONOID.zero) { case (acc, x) => MONOID.plus(acc, MONOID.create(x)) }
  }
  
  def exactFrequency(data : Seq[Long], x : Long) : Long = data.filter { _ == x }.size
  
  def approximateFrequency(cms : CMS, x : Long) : Long = {
    cms.estimateFrequency(x)
  }

  "CountMinSketch" should {
     "estimate frequencies" in {
       val totalCount = 5678
       val range = 897 
       val data = (0 to (totalCount - 1)).map { i => RAND.nextInt(range).toLong }
       val cms = createCountMinSketch(data)
       
       (0 to 100).foreach { i =>
         val x = RAND.nextInt(range).toLong
         val exact = exactFrequency(data, x)
         val approx = approximateFrequency(cms, x)
         val maxError = cms.maxErrorOfFrequencyEstimate
         approx must be_>=(exact)
         maxError must be_==((2.0 * totalCount) / WIDTH)
         (approx - exact).toDouble must be_<=(maxError)
       }
     }
     
     "exactly estimate frequencies when given a small stream" in {
       val one = MONOID.create(1)
       val two = MONOID.create(2)
       val cms = MONOID.plus(MONOID.plus(one, two), two)
       
       cms.estimateFrequency(1) must be_==(1)
       cms.estimateFrequency(2) must be_==(2)       
     }
  }
}
