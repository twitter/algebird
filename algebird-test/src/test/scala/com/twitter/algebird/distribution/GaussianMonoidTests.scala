package com.twitter.algebird

class GaussianMonoidTests extends CheckProperties { 
  import com.twitter.algebird.BaseProperties._
  val MEAN = 10
  val SIGMA = 100

  implicit def gaussianMonoid = new GaussianMonoid[Int](SIZE)



}